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
from recidiviz.calculator.query import bq_utils
from recidiviz.calculator.query.state import (
    dataset_config,
    state_specific_query_strings,
)
from recidiviz.metrics.metric_big_query_view import MetricBigQueryViewBuilder
from recidiviz.utils.environment import GCP_PROJECT_STAGING
from recidiviz.utils.metadata import local_project_id_override

INCARCERATION_RELEASES_BY_TYPE_BY_PERIOD_VIEW_NAME = (
    "incarceration_releases_by_type_by_period"
)

INCARCERATION_RELEASES_BY_TYPE_BY_PERIOD_VIEW_DESCRIPTION = (
    """Incarceration releases by period broken down by release type and demographics."""
)

# TODO(#3657): Update this query exclude US_ND releases from 'CPP' once we are classifying transfers to CPP as releases
INCARCERATION_RELEASES_BY_TYPE_BY_PERIOD_VIEW_QUERY_TEMPLATE = """
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
          {state_specific_race_or_ethnicity_groupings},
          IFNULL(gender, 'EXTERNAL_UNKNOWN') as gender,
          {age_bucket}
        FROM
          `{project_id}.{materialized_metrics_dataset}.most_recent_incarceration_release_metrics_included_in_state_population_materialized` releases
        WHERE release_reason in ('COMMUTED', 'COMPASSIONATE', 'CONDITIONAL_RELEASE', 'SENTENCE_SERVED', 'TRANSFER_TO_OTHER_JURISDICTION', 'DEATH')
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
      COUNT(DISTINCT IF(release_reason = 'TRANSFER_TO_OTHER_JURISDICTION', person_id, NULL)) as external_transfer_count,
      COUNT(DISTINCT IF(release_reason IN ('SENTENCE_SERVED', 'COMPASSIONATE', 'COMMUTED'), person_id, NULL)) as sentence_completion_count,
      COUNT(DISTINCT IF(release_reason = 'CONDITIONAL_RELEASE' AND supervision_type_at_release = 'PAROLE', person_id, NULL)) as parole_count,
      COUNT(DISTINCT IF(release_reason = 'CONDITIONAL_RELEASE' AND supervision_type_at_release = 'PROBATION', person_id, NULL)) as probation_count,
      COUNT(DISTINCT IF(release_reason = 'DEATH', person_id, NULL)) as death_count,
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

INCARCERATION_RELEASES_BY_TYPE_BY_PERIOD_VIEW_BUILDER = MetricBigQueryViewBuilder(
    dataset_id=dataset_config.PUBLIC_DASHBOARD_VIEWS_DATASET,
    view_id=INCARCERATION_RELEASES_BY_TYPE_BY_PERIOD_VIEW_NAME,
    view_query_template=INCARCERATION_RELEASES_BY_TYPE_BY_PERIOD_VIEW_QUERY_TEMPLATE,
    dimensions=(
        "state_code",
        "metric_period_months",
        "race_or_ethnicity",
        "gender",
        "age_bucket",
    ),
    description=INCARCERATION_RELEASES_BY_TYPE_BY_PERIOD_VIEW_DESCRIPTION,
    materialized_metrics_dataset=dataset_config.DATAFLOW_METRICS_MATERIALIZED_DATASET,
    metric_period_condition=bq_utils.metric_period_condition(),
    unnested_race_or_ethnicity_dimension=bq_utils.unnest_column(
        "race_or_ethnicity", "race_or_ethnicity"
    ),
    gender_dimension=bq_utils.unnest_column("gender", "gender"),
    age_dimension=bq_utils.unnest_column("age_bucket", "age_bucket"),
    state_specific_race_or_ethnicity_groupings=state_specific_query_strings.state_specific_race_or_ethnicity_groupings(
        "prioritized_race_or_ethnicity"
    ),
    age_bucket=bq_utils.age_bucket_grouping(use_external_unknown_when_null=True),
)

if __name__ == "__main__":
    with local_project_id_override(GCP_PROJECT_STAGING):
        INCARCERATION_RELEASES_BY_TYPE_BY_PERIOD_VIEW_BUILDER.build_and_print()
