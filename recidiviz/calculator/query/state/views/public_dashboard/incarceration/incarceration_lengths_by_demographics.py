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
"""Incarceration lengths in years for releases from incarceration in the last 36 months, where the release is someone's
first release from an incarceration for a new charge."""
# pylint: disable=trailing-whitespace
from recidiviz.metrics.metric_big_query_view import MetricBigQueryViewBuilder
from recidiviz.calculator.query import bq_utils
from recidiviz.calculator.query.state import (
    dataset_config,
    state_specific_query_strings,
)
from recidiviz.utils.environment import GCP_PROJECT_STAGING
from recidiviz.utils.metadata import local_project_id_override

INCARCERATION_LENGTHS_BY_DEMOGRAPHICS_VIEW_NAME = (
    "incarceration_lengths_by_demographics"
)

INCARCERATION_LENGTHS_BY_DEMOGRAPHICS_VIEW_DESCRIPTION = """Years spent incarcerated for people released from prison in the last 36 months. Release must have one of the
     following release reasons:
        COMMUTED, COMPASSIONATE, CONDITIONAL_RELEASE, SENTENCE_SERVED, DEATH
     and the original admission reason on the period of incarceration must be one of the following:
        NEW_ADMISSION, PROBATION_REVOCATION
    This methodology intends to calculate the amount of time spent incarcerated at the time of someone's "first release"
    from an incarceration for a new charge, and notably excludes time spent incarcerated following a parole revocation.     
    """

# TODO(#3657): Update this query exclude US_ND releases from 'CPP' once we are classifying transfers to CPP as releases
INCARCERATION_LENGTHS_BY_DEMOGRAPHICS_VIEW_QUERY_TEMPLATE = """
    /*{description}*/
    WITH releases AS (
        SELECT
          state_code, 
          year,
          month,
          person_id,
          release_date,
          {state_specific_race_or_ethnicity_groupings},
          IFNULL(gender, 'EXTERNAL_UNKNOWN') as gender,
          IFNULL(age_bucket, 'EXTERNAL_UNKNOWN') as age_bucket,
          IEEE_DIVIDE(total_days_incarcerated, 365.25) as years_incarcerated 
        FROM
          `{project_id}.{materialized_metrics_dataset}.most_recent_incarceration_release_metrics_materialized` releases
        WHERE release_reason in ('COMMUTED', 'COMPASSIONATE', 'CONDITIONAL_RELEASE', 'SENTENCE_SERVED', 'DEATH', 'EXECUTION')
          AND admission_reason IN ('NEW_ADMISSION', 'PROBATION_REVOCATION')
    ), ranked_releases_by_period AS (
        SELECT
          *,
          ROW_NUMBER() OVER (PARTITION BY state_code, metric_period_months, person_id ORDER BY release_date DESC) as release_ranking
        FROM
          releases,
          -- We only want a 36-month period for this view --
          UNNEST([36]) AS metric_period_months
        WHERE {metric_period_condition}
    )
    
    SELECT
      state_code,
      metric_period_months,
      race_or_ethnicity,
      gender,
      age_bucket,
      COUNT(DISTINCT IF(years_incarcerated < 1, person_id, NULL)) as years_0_1,
      COUNT(DISTINCT IF(1 <= years_incarcerated AND years_incarcerated < 2, person_id, NULL)) as years_1_2,
      COUNT(DISTINCT IF(2 <= years_incarcerated AND years_incarcerated < 3, person_id, NULL)) as years_2_3,
      COUNT(DISTINCT IF(3 <= years_incarcerated AND years_incarcerated < 5, person_id, NULL)) as years_3_5,
      COUNT(DISTINCT IF(5 <= years_incarcerated AND years_incarcerated < 10, person_id, NULL)) as years_5_10,
      COUNT(DISTINCT IF(10 <= years_incarcerated AND years_incarcerated < 20, person_id, NULL)) as years_10_20,
      COUNT(DISTINCT IF(20 <= years_incarcerated, person_id, NULL)) as years_20_plus,
      COUNT(DISTINCT(person_id)) as total_release_count
    FROM
      ranked_releases_by_period,
      {unnested_race_or_ethnicity_dimension},
      {gender_dimension},
      {age_dimension}
    WHERE release_ranking = 1
      AND ((race_or_ethnicity != 'ALL' AND gender = 'ALL' AND age_bucket = 'ALL') -- Race breakdown
      OR (race_or_ethnicity = 'ALL' AND gender != 'ALL' AND age_bucket = 'ALL') -- Gender breakdown
      OR (race_or_ethnicity = 'ALL' AND gender = 'ALL' AND age_bucket != 'ALL') -- Age breakdown
      OR (race_or_ethnicity = 'ALL' AND gender = 'ALL' AND age_bucket = 'ALL')) -- State-wide count
    GROUP BY state_code, metric_period_months, race_or_ethnicity, gender, age_bucket
    ORDER BY state_code, metric_period_months, race_or_ethnicity, gender, age_bucket
    """

INCARCERATION_LENGTHS_BY_DEMOGRAPHICS_VIEW_BUILDER = MetricBigQueryViewBuilder(
    dataset_id=dataset_config.PUBLIC_DASHBOARD_VIEWS_DATASET,
    view_id=INCARCERATION_LENGTHS_BY_DEMOGRAPHICS_VIEW_NAME,
    view_query_template=INCARCERATION_LENGTHS_BY_DEMOGRAPHICS_VIEW_QUERY_TEMPLATE,
    dimensions=[
        "state_code",
        "metric_period_months",
        "race_or_ethnicity",
        "gender",
        "age_bucket",
    ],
    description=INCARCERATION_LENGTHS_BY_DEMOGRAPHICS_VIEW_DESCRIPTION,
    materialized_metrics_dataset=dataset_config.DATAFLOW_METRICS_MATERIALIZED_DATASET,
    reference_views_dataset=dataset_config.REFERENCE_VIEWS_DATASET,
    static_reference_dataset=dataset_config.STATIC_REFERENCE_TABLES_DATASET,
    metric_period_condition=bq_utils.metric_period_condition(),
    unnested_race_or_ethnicity_dimension=bq_utils.unnest_column(
        "race_or_ethnicity", "race_or_ethnicity"
    ),
    gender_dimension=bq_utils.unnest_column("gender", "gender"),
    age_dimension=bq_utils.unnest_column("age_bucket", "age_bucket"),
    state_specific_race_or_ethnicity_groupings=state_specific_query_strings.state_specific_race_or_ethnicity_groupings(
        "prioritized_race_or_ethnicity"
    ),
)

if __name__ == "__main__":
    with local_project_id_override(GCP_PROJECT_STAGING):
        INCARCERATION_LENGTHS_BY_DEMOGRAPHICS_VIEW_BUILDER.build_and_print()
