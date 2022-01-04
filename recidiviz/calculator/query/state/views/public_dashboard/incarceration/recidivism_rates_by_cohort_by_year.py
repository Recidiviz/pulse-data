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
"""Reincarceration recidivism rates by release cohort and follow-up period years, with demographic breakdowns."""
from recidiviz.big_query.big_query_view import BigQueryAddress
from recidiviz.calculator.query import bq_utils
from recidiviz.calculator.query.state import (
    dataset_config,
    state_specific_query_strings,
)
from recidiviz.calculator.query.state.views.public_dashboard.utils import (
    spotlight_age_buckets,
)
from recidiviz.metrics.metric_big_query_view import MetricBigQueryViewBuilder
from recidiviz.utils.environment import GCP_PROJECT_STAGING
from recidiviz.utils.metadata import local_project_id_override

RECIDIVISM_RATES_BY_COHORT_BY_YEAR_VIEW_NAME = "recidivism_rates_by_cohort_by_year"

RECIDIVISM_RATES_BY_COHORT_BY_YEAR_VIEW_DESCRIPTION = """Reincarceration recidivism rates by release cohort and follow-up period years, with demographic breakdowns."""

RECIDIVISM_RATES_BY_COHORT_BY_YEAR_VIEW_QUERY_TEMPLATE = """
    /*{description}*/
    WITH releases AS (
      SELECT
        person_id,
        state_code,
        release_cohort,
        follow_up_period,
        gender,
        {age_bucket},
        prioritized_race_or_ethnicity,
        did_recidivate,
        ROW_NUMBER() OVER (PARTITION BY state_code, release_cohort, follow_up_period, person_id
          ORDER BY release_date ASC, did_recidivate DESC) as release_order
        FROM `{project_id}.{materialized_metrics_dataset}.most_recent_recidivism_rate_metrics_materialized`
      -- For 10 years of release cohorts that have at least 1 full year of follow-up -- 
      WHERE release_cohort >= EXTRACT(YEAR FROM CURRENT_DATE('US/Eastern')) - 11
      -- Only include follow-up periods that have completed --
      AND (release_cohort + follow_up_period < EXTRACT(YEAR FROM CURRENT_DATE('US/Eastern')))
      -- Exclude 'US_PA' recidivism, we use the numbers provided directly by the state
      -- as they include rearrests, per their definition of recidivism.
      AND state_code != 'US_PA'
  ), recidivism_numbers AS (
      SELECT
        state_code,
        release_cohort,
        follow_up_period as followup_years,
        IFNULL(gender, 'EXTERNAL_UNKNOWN') as gender,
        IFNULL(age_bucket, 'EXTERNAL_UNKNOWN') as age_bucket,
        {state_specific_race_or_ethnicity_groupings},
        COUNTIF(did_recidivate) as recidivated_releases,
        COUNT(DISTINCT(person_id)) as releases
      FROM releases,
        {gender_dimension},
        {age_dimension},
        {race_or_ethnicity_dimension}
      -- For 10 years of release cohorts that have at least 1 full year of follow-up -- 
      WHERE release_order = 1
      GROUP BY state_code, release_cohort, followup_years, gender, age_bucket, race_or_ethnicity
    ), unnested_pa_recidivism as (
      SELECT
        year_of_release as release_cohort,
        followup_years,
        inmates_released as releases,
        CASE followup_years
          WHEN 0.5 THEN recidivism_6mo
          WHEN 1 THEN recidivism_12mo
          WHEN 3 THEN recidivism_36mo
        END as recidivism_rate
      FROM
        `{project_id}.{pa_recidivism_dataset}.{pa_recidivism_table}`,
        UNNEST([0.5, 1, 3]) as followup_years
    ), pa_recidivism as (
      SELECT
        'US_PA' as state_code,
        release_cohort,
        followup_years,
        'ALL' as gender,
        'ALL' as age_bucket,
        'ALL' as race_or_ethnicity,
        CAST(ROUND(recidivism_rate * releases) as INT64) as recidivated_releases,
        releases,
        recidivism_rate
      FROM unnested_pa_recidivism
      -- For the last 10 release cohorts that data 
      WHERE release_cohort IN (
        SELECT DISTINCT release_cohort
        FROM unnested_pa_recidivism
        WHERE recidivism_rate IS NOT NULL
        ORDER BY release_cohort DESC
        LIMIT 10
      )
      -- Only include follow-up periods that have completed
      AND recidivism_rate IS NOT NULL
    )
    
    SELECT
      *,
      ROUND(IEEE_DIVIDE(recidivated_releases, releases), 2) as recidivism_rate
    FROM
      recidivism_numbers
    UNION ALL
    SELECT
      *
    FROM
      pa_recidivism
    ORDER BY state_code, release_cohort, followup_years, gender, age_bucket, race_or_ethnicity
    """

# TODO(#7373): Manage this table automatically.
PA_RECIDIVISM_ADDRESS = BigQueryAddress(
    dataset_id="us_pa_supplemental", table_id="recidivism"
)

RECIDIVISM_RATES_BY_COHORT_BY_YEAR_VIEW_BUILDER = MetricBigQueryViewBuilder(
    dataset_id=dataset_config.PUBLIC_DASHBOARD_VIEWS_DATASET,
    view_id=RECIDIVISM_RATES_BY_COHORT_BY_YEAR_VIEW_NAME,
    view_query_template=RECIDIVISM_RATES_BY_COHORT_BY_YEAR_VIEW_QUERY_TEMPLATE,
    dimensions=(
        "state_code",
        "release_cohort",
        "followup_years",
        "gender",
        "age_bucket",
        "race_or_ethnicity",
    ),
    description=RECIDIVISM_RATES_BY_COHORT_BY_YEAR_VIEW_DESCRIPTION,
    materialized_metrics_dataset=dataset_config.DATAFLOW_METRICS_MATERIALIZED_DATASET,
    state_specific_race_or_ethnicity_groupings=state_specific_query_strings.state_specific_race_or_ethnicity_groupings(),
    race_or_ethnicity_dimension=bq_utils.unnest_column(
        "prioritized_race_or_ethnicity", "race_or_ethnicity"
    ),
    gender_dimension=bq_utils.unnest_column("gender", "gender"),
    age_dimension=bq_utils.unnest_column("age_bucket", "age_bucket"),
    pa_recidivism_dataset=PA_RECIDIVISM_ADDRESS.dataset_id,
    pa_recidivism_table=PA_RECIDIVISM_ADDRESS.table_id,
    age_bucket=spotlight_age_buckets(),
)

if __name__ == "__main__":
    with local_project_id_override(GCP_PROJECT_STAGING):
        RECIDIVISM_RATES_BY_COHORT_BY_YEAR_VIEW_BUILDER.build_and_print()
