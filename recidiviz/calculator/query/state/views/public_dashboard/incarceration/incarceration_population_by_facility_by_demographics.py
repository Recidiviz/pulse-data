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
"""Most recent daily incarceration population count with facility and demographic breakdowns."""
# pylint: disable=trailing-whitespace, line-too-long
from recidiviz.big_query.big_query_view import SimpleBigQueryViewBuilder
from recidiviz.calculator.query import bq_utils
from recidiviz.calculator.query.state import dataset_config
from recidiviz.utils.environment import GAE_PROJECT_STAGING
from recidiviz.utils.metadata import local_project_id_override

INCARCERATION_POPULATION_BY_FACILITY_BY_DEMOGRAPHICS_VIEW_NAME = 'incarceration_population_by_facility_by_demographics'

INCARCERATION_POPULATION_BY_FACILITY_BY_DEMOGRAPHICS_VIEW_DESCRIPTION = """Most recent daily incarceration population count with facility and demographic breakdowns."""

INCARCERATION_POPULATION_BY_FACILITY_BY_DEMOGRAPHICS_VIEW_QUERY_TEMPLATE = \
    """
    /*{description}*/
    WITH most_recent_dates_by_state_code AS (
      SELECT
        state_code,
        job_id,
        date_of_stay,
        ROW_NUMBER() OVER (PARTITION BY state_code ORDER BY date_of_stay DESC) AS recency_rank
      FROM
        `{project_id}.{metrics_dataset}.incarceration_population_metrics`
      JOIN
        `{project_id}.{reference_dataset}.most_recent_job_id_by_metric_and_state_code`
      USING (state_code, job_id, year, month, metric_period_months)
      WHERE metric_period_months = 0
      AND methodology = 'PERSON'
      AND metric_type = 'INCARCERATION_POPULATION'
      AND EXTRACT(YEAR FROM date_of_stay) = EXTRACT(YEAR FROM CURRENT_DATE())
    ), most_recent_incarcerations AS (
      SELECT
        state_code,
        person_id,
        facility,
        race_or_ethnicity,
        IFNULL(gender, 'EXTERNAL_UNKNOWN') as gender,
        IFNULL(age_bucket, 'EXTERNAL_UNKNOWN') as age_bucket,
        date_of_stay,
      FROM
         most_recent_dates_by_state_code
      LEFT JOIN
        `{project_id}.{metrics_dataset}.incarceration_population_metrics`
      USING (state_code, job_id, date_of_stay),
      {race_or_ethnicity_dimension}
      WHERE recency_rank = 1
      AND metric_period_months = 0
      AND methodology = 'PERSON'
      AND (state_code != 'US_ND' OR facility not in ('OOS', 'CPP'))
    )
    
    SELECT
      state_code,
      date_of_stay,
      IFNULL(facility_shorthand, facility) as facility,
      CASE WHEN state_code = 'US_ND' AND race_or_ethnicity IN ('EXTERNAL_UNKNOWN', 'ASIAN', 'NATIVE_HAWAIIAN_PACIFIC_ISLANDER') THEN 'OTHER'
          ELSE race_or_ethnicity END AS race_or_ethnicity,
      gender,
      age_bucket,
      COUNT(DISTINCT(person_id)) as total_population
    FROM
      most_recent_incarcerations
    LEFT JOIN
      `{project_id}.{reference_dataset}.state_incarceration_facility_capacity`
    USING (state_code, facility),
      {facility_dimension},
      {unnested_race_or_ethnicity_dimension},
      {gender_dimension},
      {age_dimension}
    WHERE (race_or_ethnicity != 'ALL' AND gender = 'ALL' AND age_bucket = 'ALL') -- Race breakdown
      OR (race_or_ethnicity = 'ALL' AND gender != 'ALL' AND age_bucket = 'ALL') -- Gender breakdown
      OR (race_or_ethnicity = 'ALL' AND gender = 'ALL' AND age_bucket != 'ALL') -- Age breakdown
      OR (facility != 'ALL' AND race_or_ethnicity = 'ALL' AND gender = 'ALL' AND age_bucket = 'ALL') -- Facility breakdown
      OR (facility = 'ALL' AND race_or_ethnicity = 'ALL' AND gender = 'ALL' AND age_bucket = 'ALL') -- State-wide count
    GROUP BY state_code, date_of_stay,  facility, race_or_ethnicity, gender, age_bucket
    ORDER BY state_code, date_of_stay, facility, race_or_ethnicity, gender, age_bucket
    """

INCARCERATION_POPULATION_BY_FACILITY_BY_DEMOGRAPHICS_VIEW_BUILDER = SimpleBigQueryViewBuilder(
    dataset_id=dataset_config.PUBLIC_DASHBOARD_VIEWS_DATASET,
    view_id=INCARCERATION_POPULATION_BY_FACILITY_BY_DEMOGRAPHICS_VIEW_NAME,
    view_query_template=INCARCERATION_POPULATION_BY_FACILITY_BY_DEMOGRAPHICS_VIEW_QUERY_TEMPLATE,
    description=INCARCERATION_POPULATION_BY_FACILITY_BY_DEMOGRAPHICS_VIEW_DESCRIPTION,
    metrics_dataset=dataset_config.DATAFLOW_METRICS_DATASET,
    reference_dataset=dataset_config.REFERENCE_TABLES_DATASET,
    race_or_ethnicity_dimension=bq_utils.unnest_race_and_ethnicity(),
    metric_period_dimension=bq_utils.unnest_metric_period_months(),
    unnested_race_or_ethnicity_dimension=bq_utils.unnest_column('race_or_ethnicity', 'race_or_ethnicity'),
    gender_dimension=bq_utils.unnest_column('gender', 'gender'),
    age_dimension=bq_utils.unnest_column('age_bucket', 'age_bucket'),
    facility_dimension=bq_utils.unnest_column('facility', 'facility')
)

if __name__ == '__main__':
    with local_project_id_override(GAE_PROJECT_STAGING):
        INCARCERATION_POPULATION_BY_FACILITY_BY_DEMOGRAPHICS_VIEW_BUILDER.build_and_print()
