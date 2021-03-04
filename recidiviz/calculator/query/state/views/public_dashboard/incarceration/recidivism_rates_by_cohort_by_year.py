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
# pylint: disable=trailing-whitespace
from recidiviz.metrics.metric_big_query_view import MetricBigQueryViewBuilder
from recidiviz.calculator.query import bq_utils
from recidiviz.calculator.query.state import state_specific_query_strings
from recidiviz.calculator.query.state import dataset_config
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
        age_bucket,
        prioritized_race_or_ethnicity,
        did_recidivate,
        ROW_NUMBER() OVER (PARTITION BY state_code, release_cohort, follow_up_period, person_id
          ORDER BY release_date ASC, did_recidivate DESC) as release_order
        FROM `{project_id}.{materialized_metrics_dataset}.most_recent_recidivism_rate_metrics_materialized`
      -- For 10 years of release cohorts that have at least 1 full year of follow-up -- 
      WHERE release_cohort >= EXTRACT(YEAR FROM CURRENT_DATE()) - 11
      -- Only include follow-up periods that have completed --
      AND (release_cohort + follow_up_period < EXTRACT(YEAR FROM CURRENT_DATE()))
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
    )
    
    SELECT
      *,
      ROUND(IEEE_DIVIDE(recidivated_releases, releases), 2) as recidivism_rate
    FROM
      recidivism_numbers
    ORDER BY state_code, release_cohort, followup_years, gender, age_bucket, race_or_ethnicity
    """

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
)

if __name__ == "__main__":
    with local_project_id_override(GCP_PROJECT_STAGING):
        RECIDIVISM_RATES_BY_COHORT_BY_YEAR_VIEW_BUILDER.build_and_print()
