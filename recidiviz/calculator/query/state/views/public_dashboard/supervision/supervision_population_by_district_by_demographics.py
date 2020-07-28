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
"""Most recent daily supervision population counts broken down by district and demographic categories."""
# pylint: disable=trailing-whitespace, line-too-long
from recidiviz.big_query.big_query_view import SimpleBigQueryViewBuilder
from recidiviz.calculator.query import bq_utils
from recidiviz.calculator.query.state import dataset_config
from recidiviz.utils.environment import GAE_PROJECT_STAGING
from recidiviz.utils.metadata import local_project_id_override

SUPERVISION_POPULATION_BY_DISTRICT_BY_DEMOGRAPHICS_VIEW_NAME = 'supervision_population_by_district_by_demographics'

SUPERVISION_POPULATION_BY_DISTRICT_BY_DEMOGRAPHICS_VIEW_DESCRIPTION = \
    """Most recent daily supervision population counts broken down by district and demographic categories."""

SUPERVISION_POPULATION_BY_DISTRICT_BY_DEMOGRAPHICS_VIEW_QUERY_TEMPLATE = \
    """
    /*{description}*/
    WITH most_recent_dates_by_state_code AS (
      SELECT
        state_code,
        job_id,
        date_of_supervision,
        ROW_NUMBER() OVER (PARTITION BY state_code ORDER BY date_of_supervision DESC) AS recency_rank
      FROM
        `{project_id}.{metrics_dataset}.supervision_population_metrics`
      JOIN
        `{project_id}.{reference_dataset}.most_recent_job_id_by_metric_and_state_code`
      USING (state_code, job_id, year, month, metric_period_months)
      WHERE metric_period_months = 0
      AND methodology = 'EVENT'
      AND metric_type = 'SUPERVISION_POPULATION'
      AND EXTRACT(YEAR FROM date_of_supervision) = EXTRACT(YEAR FROM CURRENT_DATE())
    ), supervision_populations AS (
      SELECT
        state_code,
        person_id,
        supervision_type,
        date_of_supervision,
        {grouped_districts} as district,
        race_or_ethnicity,
        IFNULL(gender, 'EXTERNAL_UNKNOWN') as gender,
        IFNULL(age_bucket, 'EXTERNAL_UNKNOWN') as age_bucket
      FROM
         most_recent_dates_by_state_code
      LEFT JOIN
        `{project_id}.{metrics_dataset}.supervision_population_metrics`
      USING (state_code, job_id, date_of_supervision),
      {race_or_ethnicity_dimension}
      WHERE recency_rank = 1
      AND metric_period_months = 0
      AND methodology = 'EVENT'
      AND supervision_type IN ('PROBATION', 'PAROLE')
    )
          
    
    SELECT
      state_code,
      supervision_type,
      district,
      {state_specific_race_or_ethnicity_groupings},
      gender,
      age_bucket,
      COUNT(DISTINCT person_id) AS total_supervision_count
    FROM supervision_populations,
      {unnested_race_or_ethnicity_dimension},
      {gender_dimension},
      {age_dimension},
      {district_dimension}
    WHERE (race_or_ethnicity != 'ALL' AND gender = 'ALL' AND age_bucket = 'ALL') -- Race breakdown
      OR (race_or_ethnicity = 'ALL' AND gender != 'ALL' AND age_bucket = 'ALL') -- Gender breakdown
      OR (race_or_ethnicity = 'ALL' AND gender = 'ALL' AND age_bucket != 'ALL') -- Age breakdown
      OR (district != 'ALL' AND race_or_ethnicity = 'ALL' AND gender = 'ALL' AND age_bucket = 'ALL') -- District breakdown
      OR (district = 'ALL' AND race_or_ethnicity = 'ALL' AND gender = 'ALL' AND age_bucket = 'ALL') -- State-wide count
    GROUP BY state_code, supervision_type, district, race_or_ethnicity, gender, age_bucket
    ORDER BY state_code, supervision_type, district, race_or_ethnicity, gender, age_bucket
    """

SUPERVISION_POPULATION_BY_DISTRICT_BY_DEMOGRAPHICS_VIEW_BUILDER = SimpleBigQueryViewBuilder(
    dataset_id=dataset_config.PUBLIC_DASHBOARD_VIEWS_DATASET,
    view_id=SUPERVISION_POPULATION_BY_DISTRICT_BY_DEMOGRAPHICS_VIEW_NAME,
    view_query_template=SUPERVISION_POPULATION_BY_DISTRICT_BY_DEMOGRAPHICS_VIEW_QUERY_TEMPLATE,
    description=SUPERVISION_POPULATION_BY_DISTRICT_BY_DEMOGRAPHICS_VIEW_DESCRIPTION,
    reference_dataset=dataset_config.REFERENCE_TABLES_DATASET,
    metrics_dataset=dataset_config.DATAFLOW_METRICS_DATASET,
    grouped_districts=bq_utils.supervision_specific_district_groupings('supervising_district_external_id', 'judicial_district_code'),
    race_or_ethnicity_dimension=bq_utils.unnest_race_and_ethnicity(),
    current_month_condition=bq_utils.current_month_condition(),
    unnested_race_or_ethnicity_dimension=bq_utils.unnest_column('race_or_ethnicity', 'race_or_ethnicity'),
    gender_dimension=bq_utils.unnest_column('gender', 'gender'),
    age_dimension=bq_utils.unnest_column('age_bucket', 'age_bucket'),
    district_dimension=bq_utils.unnest_district('district'),
    state_specific_race_or_ethnicity_groupings=bq_utils.state_specific_race_or_ethnicity_groupings()
)

if __name__ == '__main__':
    with local_project_id_override(GAE_PROJECT_STAGING):
        SUPERVISION_POPULATION_BY_DISTRICT_BY_DEMOGRAPHICS_VIEW_BUILDER.build_and_print()
