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


INCARCERATION_POPULATION_BY_PRIORITIZED_RACE_AND_ETHNICITY_BY_PERIOD_VIEW_QUERY_TEMPLATE = \
    """
    /*{description}*/
    WITH population_with_race_or_ethnicities AS (
      SELECT
        DISTINCT state_code,
        metric_period_months,
        person_id,
        prioritized_race_or_ethnicity as race_or_ethnicity,
      FROM `{project_id}.{materialized_metrics_dataset}.most_recent_incarceration_population_metrics_materialized`,
            -- We only want a 36-month period for this view --
      UNNEST ([36]) AS metric_period_months
      WHERE {metric_period_condition}
        AND {state_specific_facility_exclusion}
    )
    
    SELECT
      state_code,
      metric_period_months,
      {state_specific_race_or_ethnicity_groupings},
      COUNT(DISTINCT(person_id)) as population_count
    FROM
      population_with_race_or_ethnicities,
      {unnested_race_or_ethnicity_dimension}
    GROUP BY state_code, metric_period_months, race_or_ethnicity 
    ORDER BY state_code, metric_period_months, race_or_ethnicity 
    """

INCARCERATION_POPULATION_BY_PRIORITIZED_RACE_AND_ETHNICITY_BY_PERIOD_VIEW_BUILDER = MetricBigQueryViewBuilder(
    dataset_id=dataset_config.PUBLIC_DASHBOARD_VIEWS_DATASET,
    view_id=INCARCERATION_POPULATION_BY_PRIORITIZED_RACE_AND_ETHNICITY_BY_PERIOD_VIEW_NAME,
    view_query_template=INCARCERATION_POPULATION_BY_PRIORITIZED_RACE_AND_ETHNICITY_BY_PERIOD_VIEW_QUERY_TEMPLATE,
    dimensions=['state_code', 'metric_period_months', 'race_or_ethnicity'],
    description=INCARCERATION_POPULATION_BY_PRIORITIZED_RACE_AND_ETHNICITY_BY_PERIOD_VIEW_DESCRIPTION,
    materialized_metrics_dataset=dataset_config.DATAFLOW_METRICS_MATERIALIZED_DATASET,
    metric_period_condition=bq_utils.metric_period_condition(),
    unnested_race_or_ethnicity_dimension=bq_utils.unnest_column('race_or_ethnicity', 'race_or_ethnicity'),
    state_specific_race_or_ethnicity_groupings=state_specific_query_strings.state_specific_race_or_ethnicity_groupings(),
    state_specific_facility_exclusion=state_specific_query_strings.state_specific_facility_exclusion(),
)

if __name__ == '__main__':
    with local_project_id_override(GCP_PROJECT_STAGING):
        INCARCERATION_POPULATION_BY_PRIORITIZED_RACE_AND_ETHNICITY_BY_PERIOD_VIEW_BUILDER.build_and_print()
