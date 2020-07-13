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
from recidiviz.big_query.big_query_view import SimpleBigQueryViewBuilder
from recidiviz.calculator.query import bq_utils
from recidiviz.calculator.query.state import dataset_config
from recidiviz.utils.environment import GAE_PROJECT_STAGING
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
        IFNULL(gender, 'EXTERNAL_UNKNOWN') as gender,
        race_or_ethnicity,
        IFNULL(age_bucket, 'EXTERNAL_UNKNOWN') as age_bucket,
        supervision_type,
        metric_period_months
      FROM `{project_id}.{metrics_dataset}.supervision_success_metrics`
      JOIN `{project_id}.{reference_dataset}.most_recent_job_id_by_metric_and_state_code` job
        USING (state_code, job_id, year, month, metric_period_months),
      {race_or_ethnicity_dimension},
      {gender_dimension},
      {age_dimension},
      {metric_period_dimension}
      WHERE methodology = 'EVENT'
        AND person_id IS NOT NULL
        AND DATE(year, month, 1) >= DATE_SUB(DATE_TRUNC(CURRENT_DATE('US/Pacific'), MONTH),
                                             INTERVAL metric_period_months - 1 MONTH)
        AND job.metric_type = 'SUPERVISION_SUCCESS'
      GROUP BY state_code, person_id, metric_period_months, gender, race_or_ethnicity, age_bucket, supervision_type
    ), successful_termination_counts AS (
      SELECT
          state_code,
          metric_period_months,
          supervision_type,
          CASE WHEN state_code = 'US_ND' AND race_or_ethnicity IN ('EXTERNAL_UNKNOWN', 'ASIAN', 'NATIVE_HAWAIIAN_PACIFIC_ISLANDER') THEN 'OTHER'
          ELSE race_or_ethnicity END AS race_or_ethnicity,
          gender,
          age_bucket,
          COUNT(DISTINCT IF(successful_termination = 1, person_id, NULL)) as successful_termination_count,
          COUNT(DISTINCT person_id) as projected_completion_count
      FROM
          supervision_completions,
          {unnested_race_or_ethnicity_dimension}
      GROUP BY state_code, metric_period_months, supervision_type, race_or_ethnicity, gender, age_bucket
    )
    
    SELECT
      *,
      IEEE_DIVIDE(successful_termination_count, projected_completion_count) as success_rate
    FROM
      successful_termination_counts 
    WHERE
      (race_or_ethnicity != 'ALL' AND gender = 'ALL' AND age_bucket = 'ALL') -- Race breakdown
      OR (race_or_ethnicity = 'ALL' AND gender != 'ALL' AND age_bucket = 'ALL') -- Gender breakdown
      OR (race_or_ethnicity = 'ALL' AND gender = 'ALL' AND age_bucket != 'ALL') -- Age breakdown
      OR (race_or_ethnicity = 'ALL' AND gender = 'ALL' AND age_bucket = 'ALL') -- Overall success rate
    ORDER BY state_code, supervision_type, metric_period_months, race_or_ethnicity, gender, age_bucket
    """

SUPERVISION_SUCCESS_BY_PERIOD_BY_DEMOGRAPHICS_VIEW_BUILDER = SimpleBigQueryViewBuilder(
    dataset_id=dataset_config.PUBLIC_DASHBOARD_VIEWS_DATASET,
    view_id=SUPERVISION_SUCCESS_BY_PERIOD_BY_DEMOGRAPHICS_VIEW_NAME,
    view_query_template=SUPERVISION_SUCCESS_BY_PERIOD_BY_DEMOGRAPHICS_VIEW_QUERY_TEMPLATE,
    description=SUPERVISION_SUCCESS_BY_PERIOD_BY_DEMOGRAPHICS_VIEW_DESCRIPTION,
    metrics_dataset=dataset_config.DATAFLOW_METRICS_DATASET,
    reference_dataset=dataset_config.REFERENCE_TABLES_DATASET,
    race_or_ethnicity_dimension=bq_utils.unnest_race_and_ethnicity(),
    metric_period_dimension=bq_utils.unnest_metric_period_months(),
    unnested_race_or_ethnicity_dimension=bq_utils.unnest_column('race_or_ethnicity', 'race_or_ethnicity'),
    gender_dimension=bq_utils.unnest_column('gender', 'gender'),
    age_dimension=bq_utils.unnest_column('age_bucket', 'age_bucket')
)

if __name__ == '__main__':
    with local_project_id_override(GAE_PROJECT_STAGING):
        SUPERVISION_SUCCESS_BY_PERIOD_BY_DEMOGRAPHICS_VIEW_BUILDER.build_and_print()
