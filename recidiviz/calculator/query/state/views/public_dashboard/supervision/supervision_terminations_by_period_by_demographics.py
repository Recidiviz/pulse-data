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
"""Rates of successful terminations of supervision by period, grouped by demographic dimensions."""
from recidiviz.calculator.query import bq_utils
from recidiviz.calculator.query.state import (
    dataset_config,
    state_specific_query_strings,
)
from recidiviz.metrics.metric_big_query_view import MetricBigQueryViewBuilder
from recidiviz.utils.environment import GCP_PROJECT_STAGING
from recidiviz.utils.metadata import local_project_id_override

SUPERVISION_TERMINATIONS_BY_PERIOD_BY_DEMOGRAPHICS_VIEW_NAME = (
    "supervision_terminations_by_period_by_demographics"
)

SUPERVISION_TERMINATIONS_BY_PERIOD_BY_DEMOGRAPHICS_VIEW_DESCRIPTION = """Rates of successful terminations of supervision by period, grouped by demographic dimensions.

This is typically used as a backup for the corresponding supervision_success* view when that view produces unreliable data (e.g. when the sentencing data in a state is inaccurate)."""

SUPERVISION_TERMINATIONS_BY_PERIOD_BY_DEMOGRAPHICS_VIEW_QUERY_TEMPLATE = """
    /*{description}*/
    WITH supervision_terminations AS (
      SELECT 
        state_code,
        LOGICAL_AND(termination_reason in ('COMMUTED', 'DISCHARGE','DISMISSED','EXPIRATION','PARDONED')) as successful_termination,
        person_id,
        IFNULL(district, 'EXTERNAL_UNKNOWN') as district,
        IFNULL(gender, 'EXTERNAL_UNKNOWN') as gender,
        {state_specific_race_or_ethnicity_groupings},
        IFNULL(age_bucket, 'EXTERNAL_UNKNOWN') as age_bucket,
        supervision_type,
        metric_period_months
      FROM `{project_id}.{materialized_metrics_dataset}.most_recent_supervision_termination_metrics_materialized` termination_metrics,
      {gender_dimension},
      {age_dimension},
      UNNEST ([{grouped_districts}, 'ALL']) AS district,
      -- We only want a 36-month period for this view --
      UNNEST ([36]) AS metric_period_months
      WHERE {metric_period_condition} AND termination_reason NOT IN (
        'DEATH',
        'EXTERNAL_UNKNOWN',
        'INTERNAL_UNKNOWN',
        'RETURN_FROM_ABSCONSION',
        'SUSPENSION',
        'TRANSFER_WITHIN_STATE',
        'TRANSFER_OUT_OF_STATE')
      GROUP BY state_code, person_id, metric_period_months, district, gender, race_or_ethnicity, age_bucket, supervision_type
    ), successful_termination_counts AS (
      SELECT
          state_code,
          metric_period_months,
          district,
          supervision_type,
          race_or_ethnicity,
          gender,
          age_bucket,
          COUNT(DISTINCT IF(successful_termination, person_id, NULL)) as successful_termination_count,
          COUNT(DISTINCT person_id) as termination_count
      FROM
          supervision_terminations,
          {unnested_race_or_ethnicity_dimension}
      GROUP BY state_code, metric_period_months, district, supervision_type, race_or_ethnicity, gender, age_bucket
    )
    
    SELECT
      *,
      IEEE_DIVIDE(successful_termination_count, termination_count) as success_rate
    FROM
      successful_termination_counts 
    WHERE
      ((race_or_ethnicity != 'ALL' AND gender = 'ALL' AND age_bucket = 'ALL') -- Race breakdown
      OR (race_or_ethnicity = 'ALL' AND gender != 'ALL' AND age_bucket = 'ALL') -- Gender breakdown
      OR (race_or_ethnicity = 'ALL' AND gender = 'ALL' AND age_bucket != 'ALL') -- Age breakdown
      OR (race_or_ethnicity = 'ALL' AND gender = 'ALL' AND age_bucket = 'ALL')) -- Overall success rate
      AND {state_specific_supervision_type_inclusion_filter}
    ORDER BY state_code, supervision_type, metric_period_months, district, race_or_ethnicity, gender, age_bucket
    """

SUPERVISION_TERMINATIONS_BY_PERIOD_BY_DEMOGRAPHICS_VIEW_BUILDER = MetricBigQueryViewBuilder(
    dataset_id=dataset_config.PUBLIC_DASHBOARD_VIEWS_DATASET,
    view_id=SUPERVISION_TERMINATIONS_BY_PERIOD_BY_DEMOGRAPHICS_VIEW_NAME,
    view_query_template=SUPERVISION_TERMINATIONS_BY_PERIOD_BY_DEMOGRAPHICS_VIEW_QUERY_TEMPLATE,
    dimensions=(
        "state_code",
        "supervision_type",
        "metric_period_months",
        "district",
        "race_or_ethnicity",
        "gender",
        "age_bucket",
    ),
    description=SUPERVISION_TERMINATIONS_BY_PERIOD_BY_DEMOGRAPHICS_VIEW_DESCRIPTION,
    materialized_metrics_dataset=dataset_config.DATAFLOW_METRICS_MATERIALIZED_DATASET,
    reference_views_dataset=dataset_config.REFERENCE_VIEWS_DATASET,
    grouped_districts=state_specific_query_strings.state_supervision_specific_district_groupings(
        "supervising_district_external_id", "judicial_district_code"
    ),
    metric_period_condition=bq_utils.metric_period_condition(month_offset=1),
    unnested_race_or_ethnicity_dimension=bq_utils.unnest_column(
        "race_or_ethnicity", "race_or_ethnicity"
    ),
    gender_dimension=bq_utils.unnest_column("gender", "gender"),
    age_dimension=bq_utils.unnest_column("age_bucket", "age_bucket"),
    state_specific_race_or_ethnicity_groupings=state_specific_query_strings.state_specific_race_or_ethnicity_groupings(
        "prioritized_race_or_ethnicity"
    ),
    state_specific_supervision_type_inclusion_filter=state_specific_query_strings.state_specific_supervision_type_inclusion_filter(),
)

if __name__ == "__main__":
    with local_project_id_override(GCP_PROJECT_STAGING):
        SUPERVISION_TERMINATIONS_BY_PERIOD_BY_DEMOGRAPHICS_VIEW_BUILDER.build_and_print()
