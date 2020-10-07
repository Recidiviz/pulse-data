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
"""Rates of successful completion of supervision by period, grouped by demographic dimensions."""
# pylint: disable=trailing-whitespace, line-too-long
from recidiviz.metrics.metric_big_query_view import MetricBigQueryViewBuilder
from recidiviz.calculator.query import bq_utils
from recidiviz.calculator.query.state import dataset_config, state_specific_query_strings
from recidiviz.utils.environment import GCP_PROJECT_STAGING
from recidiviz.utils.metadata import local_project_id_override

SUPERVISION_SUCCESS_BY_PERIOD_BY_DEMOGRAPHICS_VIEW_NAME = 'supervision_success_by_period_by_demographics'

SUPERVISION_SUCCESS_BY_PERIOD_BY_DEMOGRAPHICS_VIEW_DESCRIPTION = \
    """Rates of successful completion of supervision by period, grouped by demographic dimensions."""

SUPERVISION_SUCCESS_BY_PERIOD_BY_DEMOGRAPHICS_VIEW_QUERY_TEMPLATE = \
    """
    /*{description}*/
    WITH supervision_completions AS (
      SELECT 
        state_code,
        MIN(successful_completion_count) as successful_termination,
        person_id,
        IFNULL(district, 'EXTERNAL_UNKNOWN') as district,
        IFNULL(gender, 'EXTERNAL_UNKNOWN') as gender,
        race_or_ethnicity,
        IFNULL(age_bucket, 'EXTERNAL_UNKNOWN') as age_bucket,
        supervision_type,
        metric_period_months
      FROM `{project_id}.{metrics_dataset}.supervision_success_metrics` success_metrics
      {filter_to_most_recent_job_id_for_metric},
      {race_or_ethnicity_dimension},
      {gender_dimension},
      {age_dimension},
      UNNEST ([{grouped_districts}, 'ALL']) AS district,
      -- We only want a 36-month period for this view --
      UNNEST ([36]) AS metric_period_months
      WHERE success_metrics.metric_period_months = 1
        AND {metric_period_condition}
        AND methodology = 'EVENT'
        AND person_id IS NOT NULL
      GROUP BY state_code, person_id, metric_period_months, district, gender, race_or_ethnicity, age_bucket, supervision_type
    ), terminations_with_race_or_ethnicity_priorities AS (
       SELECT
        *,
        ROW_NUMBER () OVER
            -- People can be counted on multiple types of supervision and in multiple districts simultaneously. --
            -- Partitioning over gender and age_bucket have no effect. --
            (PARTITION BY state_code, person_id, metric_period_months, supervision_type, district, gender, age_bucket
             ORDER BY representation_priority) as inclusion_priority
       FROM
          supervision_completions
       LEFT JOIN
        `{project_id}.{static_reference_dataset}.state_race_ethnicity_population_counts`
      USING (state_code, race_or_ethnicity) 
    ), successful_termination_counts AS (
      SELECT
          state_code,
          metric_period_months,
          district,
          supervision_type,
          {state_specific_race_or_ethnicity_groupings},
          gender,
          age_bucket,
          COUNT(DISTINCT IF(successful_termination = 1, person_id, NULL)) as successful_termination_count,
          COUNT(DISTINCT person_id) as projected_completion_count
      FROM
          terminations_with_race_or_ethnicity_priorities,
          {unnested_race_or_ethnicity_dimension}
      WHERE inclusion_priority = 1
      GROUP BY state_code, metric_period_months, district, supervision_type, race_or_ethnicity, gender, age_bucket
    )
    
    SELECT
      *,
      IEEE_DIVIDE(successful_termination_count, projected_completion_count) as success_rate
    FROM
      successful_termination_counts 
    WHERE
      ((race_or_ethnicity != 'ALL' AND gender = 'ALL' AND age_bucket = 'ALL') -- Race breakdown
      OR (race_or_ethnicity = 'ALL' AND gender != 'ALL' AND age_bucket = 'ALL') -- Gender breakdown
      OR (race_or_ethnicity = 'ALL' AND gender = 'ALL' AND age_bucket != 'ALL') -- Age breakdown
      OR (race_or_ethnicity = 'ALL' AND gender = 'ALL' AND age_bucket = 'ALL')) -- Overall success rate
    ORDER BY state_code, supervision_type, metric_period_months, district, race_or_ethnicity, gender, age_bucket
    """

SUPERVISION_SUCCESS_BY_PERIOD_BY_DEMOGRAPHICS_VIEW_BUILDER = MetricBigQueryViewBuilder(
    dataset_id=dataset_config.PUBLIC_DASHBOARD_VIEWS_DATASET,
    view_id=SUPERVISION_SUCCESS_BY_PERIOD_BY_DEMOGRAPHICS_VIEW_NAME,
    view_query_template=SUPERVISION_SUCCESS_BY_PERIOD_BY_DEMOGRAPHICS_VIEW_QUERY_TEMPLATE,
    dimensions=['state_code', 'supervision_type', 'metric_period_months', 'district',
                'race_or_ethnicity', 'gender', 'age_bucket'],
    description=SUPERVISION_SUCCESS_BY_PERIOD_BY_DEMOGRAPHICS_VIEW_DESCRIPTION,
    metrics_dataset=dataset_config.DATAFLOW_METRICS_DATASET,
    reference_views_dataset=dataset_config.REFERENCE_VIEWS_DATASET,
    static_reference_dataset=dataset_config.STATIC_REFERENCE_TABLES_DATASET,
    grouped_districts=
    state_specific_query_strings.state_supervision_specific_district_groupings('supervising_district_external_id', 'judicial_district_code'),
    race_or_ethnicity_dimension=bq_utils.unnest_race_and_ethnicity(),
    metric_period_condition=bq_utils.metric_period_condition(month_offset=1),
    unnested_race_or_ethnicity_dimension=bq_utils.unnest_column('race_or_ethnicity', 'race_or_ethnicity'),
    gender_dimension=bq_utils.unnest_column('gender', 'gender'),
    age_dimension=bq_utils.unnest_column('age_bucket', 'age_bucket'),
    state_specific_race_or_ethnicity_groupings=state_specific_query_strings.state_specific_race_or_ethnicity_groupings(),
    filter_to_most_recent_job_id_for_metric=bq_utils.filter_to_most_recent_job_id_for_metric(
        reference_dataset=dataset_config.REFERENCE_VIEWS_DATASET)
)

if __name__ == '__main__':
    with local_project_id_override(GCP_PROJECT_STAGING):
        SUPERVISION_SUCCESS_BY_PERIOD_BY_DEMOGRAPHICS_VIEW_BUILDER.build_and_print()
