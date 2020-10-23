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
"""Aggregate incarceration population by metric period, broken down by race/ethnicity, where a person is counted towards
only the race/ethnicity category that is least represented in the population of the state."""
# pylint: disable=trailing-whitespace, line-too-long
from recidiviz.metrics.metric_big_query_view import MetricBigQueryViewBuilder
from recidiviz.calculator.query import bq_utils
from recidiviz.calculator.query.state import dataset_config, state_specific_query_strings
from recidiviz.utils.environment import GCP_PROJECT_STAGING
from recidiviz.utils.metadata import local_project_id_override

INCARCERATION_POPULATION_BY_PRIORITIZED_RACE_AND_ETHNICITY_BY_PERIOD_VIEW_NAME = \
    'incarceration_population_by_prioritized_race_and_ethnicity_by_period'

INCARCERATION_POPULATION_BY_PRIORITIZED_RACE_AND_ETHNICITY_BY_PERIOD_VIEW_DESCRIPTION = """..."""


# TODO(#4294): Use the prioritized_race_or_ethnicity column
INCARCERATION_POPULATION_BY_PRIORITIZED_RACE_AND_ETHNICITY_BY_PERIOD_VIEW_QUERY_TEMPLATE = \
    """
    /*{description}*/
    WITH population_with_race_or_ethnicities AS (
      SELECT
        DISTINCT state_code,
        metric_period_months,
        person_id,
        race_or_ethnicity
      FROM `{project_id}.{metrics_dataset}.incarceration_population_metrics`
      {filter_to_most_recent_job_id_for_metric},
            -- We only want a 36-month period for this view --
      UNNEST ([36]) AS metric_period_months,
      {race_or_ethnicity_dimension}
      WHERE {metric_period_condition}
        AND methodology = 'EVENT'
        AND person_id IS NOT NULL
        {state_specific_facility_exclusion}
    ), population_with_race_or_ethnicity_priorities AS (
      SELECT
        *,
        ROW_NUMBER() OVER (PARTITION BY state_code, metric_period_months, person_id ORDER BY representation_priority) as inclusion_priority
      FROM population_with_race_or_ethnicities
      LEFT JOIN
         `{project_id}.{static_reference_dataset}.state_race_ethnicity_population_counts`
      USING (state_code, race_or_ethnicity)
    )
    
    SELECT
      state_code,
      metric_period_months,
      {state_specific_race_or_ethnicity_groupings},
      COUNT(DISTINCT(person_id)) as population_count
    FROM
      population_with_race_or_ethnicity_priorities,
      {unnested_race_or_ethnicity_dimension}
    WHERE inclusion_priority = 1
    GROUP BY state_code, metric_period_months, race_or_ethnicity 
    ORDER BY state_code, metric_period_months, race_or_ethnicity 
    """

INCARCERATION_POPULATION_BY_PRIORITIZED_RACE_AND_ETHNICITY_BY_PERIOD_VIEW_BUILDER = MetricBigQueryViewBuilder(
    dataset_id=dataset_config.PUBLIC_DASHBOARD_VIEWS_DATASET,
    view_id=INCARCERATION_POPULATION_BY_PRIORITIZED_RACE_AND_ETHNICITY_BY_PERIOD_VIEW_NAME,
    view_query_template=INCARCERATION_POPULATION_BY_PRIORITIZED_RACE_AND_ETHNICITY_BY_PERIOD_VIEW_QUERY_TEMPLATE,
    dimensions=['state_code', 'metric_period_months', 'race_or_ethnicity'],
    description=INCARCERATION_POPULATION_BY_PRIORITIZED_RACE_AND_ETHNICITY_BY_PERIOD_VIEW_DESCRIPTION,
    metrics_dataset=dataset_config.DATAFLOW_METRICS_DATASET,
    reference_views_dataset=dataset_config.REFERENCE_VIEWS_DATASET,
    static_reference_dataset=dataset_config.STATIC_REFERENCE_TABLES_DATASET,
    metric_period_condition=bq_utils.metric_period_condition(),
    race_or_ethnicity_dimension=bq_utils.unnest_race_and_ethnicity(),
    unnested_race_or_ethnicity_dimension=bq_utils.unnest_column('race_or_ethnicity', 'race_or_ethnicity'),
    state_specific_race_or_ethnicity_groupings=state_specific_query_strings.state_specific_race_or_ethnicity_groupings(),
    state_specific_facility_exclusion=state_specific_query_strings.state_specific_facility_exclusion(),
    filter_to_most_recent_job_id_for_metric=bq_utils.filter_to_most_recent_job_id_for_metric(
        reference_dataset=dataset_config.REFERENCE_VIEWS_DATASET)
)

if __name__ == '__main__':
    with local_project_id_override(GCP_PROJECT_STAGING):
        INCARCERATION_POPULATION_BY_PRIORITIZED_RACE_AND_ETHNICITY_BY_PERIOD_VIEW_BUILDER.build_and_print()
