# Recidiviz - a data platform for criminal justice reform
# Copyright (C) 2020 Recidiviz, Inc.
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
"""Incarceration releases by period broken down by release type and demographics."""
# pylint: disable=trailing-whitespace, line-too-long
from recidiviz.metrics.metric_big_query_view import MetricBigQueryViewBuilder
from recidiviz.calculator.query import bq_utils
from recidiviz.calculator.query.state import dataset_config, state_specific_query_strings
from recidiviz.utils.environment import GCP_PROJECT_STAGING
from recidiviz.utils.metadata import local_project_id_override

INCARCERATION_RELEASES_BY_TYPE_BY_PERIOD_VIEW_NAME = 'incarceration_releases_by_type_by_period'

INCARCERATION_RELEASES_BY_TYPE_BY_PERIOD_VIEW_DESCRIPTION = \
    """Incarceration releases by period broken down by release type and demographics."""

# TODO(#3657): Update this query exclude US_ND releases from 'CPP' once we are classifying transfers to CPP as releases
INCARCERATION_RELEASES_BY_TYPE_BY_PERIOD_VIEW_QUERY_TEMPLATE = \
    """
    /*{description}*/
    WITH releases AS (
        SELECT
          state_code, 
          year,
          month,
          person_id,
          release_date,
          release_reason,
          supervision_type_at_release,
          race,
          ethnicity,
          IFNULL(gender, 'EXTERNAL_UNKNOWN') as gender,
          IFNULL(age_bucket, 'EXTERNAL_UNKNOWN') as age_bucket
        FROM
          `{project_id}.{metrics_dataset}.incarceration_release_metrics` releases
        {filter_to_most_recent_job_id_for_metric}
        WHERE release_reason in ('COMMUTED', 'COMPASSIONATE', 'CONDITIONAL_RELEASE', 'SENTENCE_SERVED', 'TRANSFERRED_OUT_OF_STATE', 'DEATH')
          AND methodology = 'EVENT'
          AND metric_period_months = 1
    ), ranked_releases_by_period AS (
        SELECT
          *,
          ROW_NUMBER() OVER (PARTITION BY state_code, metric_period_months, person_id ORDER BY release_date DESC) as release_ranking
        FROM
          releases,
          -- We only want a 36-month period for this view --
          UNNEST([36]) AS metric_period_months
        WHERE {metric_period_condition}
    ), releases_by_period AS (
      SELECT
        * EXCEPT(race, ethnicity, release_ranking),
        ROW_NUMBER () OVER (PARTITION BY state_code, metric_period_months, person_id ORDER BY representation_priority) as inclusion_priority
      FROM
        ranked_releases_by_period,
        {race_or_ethnicity_dimension}
      LEFT JOIN
        `{project_id}.{static_reference_dataset}.state_race_ethnicity_population_counts`
      USING (state_code, race_or_ethnicity)
      WHERE release_ranking = 1
    )
    
    SELECT
      state_code,
      metric_period_months,
      {state_specific_race_or_ethnicity_groupings},
      gender,
      age_bucket,
      COUNT(DISTINCT IF(release_reason = 'TRANSFERRED_OUT_OF_STATE', person_id, NULL)) as external_transfer_count,
      COUNT(DISTINCT IF(release_reason IN ('SENTENCE_SERVED', 'COMPASSIONATE', 'COMMUTED'), person_id, NULL)) as sentence_completion_count,
      COUNT(DISTINCT IF(release_reason = 'CONDITIONAL_RELEASE' AND supervision_type_at_release = 'PAROLE', person_id, NULL)) as parole_count,
      COUNT(DISTINCT IF(release_reason = 'CONDITIONAL_RELEASE' AND supervision_type_at_release = 'PROBATION', person_id, NULL)) as probation_count,
      COUNT(DISTINCT IF(release_reason = 'DEATH', person_id, NULL)) as death_count,
      COUNT(DISTINCT(person_id)) as total_release_count
    FROM
      releases_by_period,
      {unnested_race_or_ethnicity_dimension},
      {gender_dimension},
      {age_dimension}
    WHERE inclusion_priority = 1
      AND ((race_or_ethnicity != 'ALL' AND gender = 'ALL' AND age_bucket = 'ALL') -- Race breakdown
      OR (race_or_ethnicity = 'ALL' AND gender != 'ALL' AND age_bucket = 'ALL') -- Gender breakdown
      OR (race_or_ethnicity = 'ALL' AND gender = 'ALL' AND age_bucket != 'ALL') -- Age breakdown
      OR (race_or_ethnicity = 'ALL' AND gender = 'ALL' AND age_bucket = 'ALL')) -- State-wide count
    GROUP BY state_code, metric_period_months, race_or_ethnicity, gender, age_bucket
    ORDER BY state_code, metric_period_months, race_or_ethnicity, gender, age_bucket
    """

INCARCERATION_RELEASES_BY_TYPE_BY_PERIOD_VIEW_BUILDER = MetricBigQueryViewBuilder(
    dataset_id=dataset_config.PUBLIC_DASHBOARD_VIEWS_DATASET,
    view_id=INCARCERATION_RELEASES_BY_TYPE_BY_PERIOD_VIEW_NAME,
    view_query_template=INCARCERATION_RELEASES_BY_TYPE_BY_PERIOD_VIEW_QUERY_TEMPLATE,
    dimensions=['state_code', 'metric_period_months', 'race_or_ethnicity', 'gender', 'age_bucket'],
    description=INCARCERATION_RELEASES_BY_TYPE_BY_PERIOD_VIEW_DESCRIPTION,
    metrics_dataset=dataset_config.DATAFLOW_METRICS_DATASET,
    reference_views_dataset=dataset_config.REFERENCE_VIEWS_DATASET,
    static_reference_dataset=dataset_config.STATIC_REFERENCE_TABLES_DATASET,
    metric_period_condition=bq_utils.metric_period_condition(),
    race_or_ethnicity_dimension=bq_utils.unnest_race_and_ethnicity(),
    unnested_race_or_ethnicity_dimension=bq_utils.unnest_column('race_or_ethnicity', 'race_or_ethnicity'),
    gender_dimension=bq_utils.unnest_column('gender', 'gender'),
    age_dimension=bq_utils.unnest_column('age_bucket', 'age_bucket'),
    state_specific_race_or_ethnicity_groupings=state_specific_query_strings.state_specific_race_or_ethnicity_groupings(),
    filter_to_most_recent_job_id_for_metric=bq_utils.filter_to_most_recent_job_id_for_metric(
        reference_dataset=dataset_config.REFERENCE_VIEWS_DATASET)
)

if __name__ == '__main__':
    with local_project_id_override(GCP_PROJECT_STAGING):
        INCARCERATION_RELEASES_BY_TYPE_BY_PERIOD_VIEW_BUILDER.build_and_print()
