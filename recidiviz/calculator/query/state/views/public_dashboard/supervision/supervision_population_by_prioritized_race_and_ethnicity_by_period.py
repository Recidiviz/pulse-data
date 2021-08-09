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
"""Supervision population by metric period, broken down by race/ethnicity, where a person is counted towards only
the race/ethnicity category that is least represented in the population of the state."""
from recidiviz.calculator.query import bq_utils
from recidiviz.calculator.query.state import (
    dataset_config,
    state_specific_query_strings,
)
from recidiviz.metrics.metric_big_query_view import MetricBigQueryViewBuilder
from recidiviz.utils.environment import GCP_PROJECT_STAGING
from recidiviz.utils.metadata import local_project_id_override

SUPERVISION_POPULATION_BY_PRIORITIZED_RACE_AND_ETHNICITY_BY_PERIOD_VIEW_NAME = (
    "supervision_population_by_prioritized_race_and_ethnicity_by_period"
)

SUPERVISION_POPULATION_BY_PRIORITIZED_RACE_AND_ETHNICITY_BY_PERIOD_VIEW_DESCRIPTION = """Supervision population by metric period, broken down by race/ethnicity, where a person is counted towards only
    the race/ethnicity category that is least represented in the population of the state."""


SUPERVISION_POPULATION_BY_PRIORITIZED_RACE_AND_ETHNICITY_BY_PERIOD_VIEW_QUERY_TEMPLATE = """
    /*{description}*/

    SELECT
      state_code,
      metric_period_months,
      supervision_type,
      {state_specific_race_or_ethnicity_groupings},
      COUNT(DISTINCT(person_id)) AS total_supervision_population
    FROM
      `{project_id}.{reference_views_dataset}.event_based_supervision_populations`,
      {unnested_race_or_ethnicity_dimension},
    UNNEST ([36]) AS metric_period_months
    WHERE {metric_period_condition}
      AND supervision_type IN ('ALL', 'PAROLE', 'PROBATION')
      AND {state_specific_supervision_type_inclusion_filter}
    GROUP BY state_code, metric_period_months, supervision_type, race_or_ethnicity
    ORDER BY state_code, metric_period_months, supervision_type, race_or_ethnicity
    """

SUPERVISION_POPULATION_BY_PRIORITIZED_RACE_AND_ETHNICITY_BY_PERIOD_VIEW_BUILDER = MetricBigQueryViewBuilder(
    dataset_id=dataset_config.PUBLIC_DASHBOARD_VIEWS_DATASET,
    view_id=SUPERVISION_POPULATION_BY_PRIORITIZED_RACE_AND_ETHNICITY_BY_PERIOD_VIEW_NAME,
    view_query_template=SUPERVISION_POPULATION_BY_PRIORITIZED_RACE_AND_ETHNICITY_BY_PERIOD_VIEW_QUERY_TEMPLATE,
    dimensions=(
        "state_code",
        "supervision_type",
        "metric_period_months",
        "race_or_ethnicity",
    ),
    description=SUPERVISION_POPULATION_BY_PRIORITIZED_RACE_AND_ETHNICITY_BY_PERIOD_VIEW_DESCRIPTION,
    reference_views_dataset=dataset_config.REFERENCE_VIEWS_DATASET,
    static_reference_dataset=dataset_config.STATIC_REFERENCE_TABLES_DATASET,
    metric_period_condition=bq_utils.metric_period_condition(),
    unnested_race_or_ethnicity_dimension=bq_utils.unnest_column(
        "race_or_ethnicity", "race_or_ethnicity"
    ),
    gender_dimension=bq_utils.unnest_column("gender", "gender"),
    age_dimension=bq_utils.unnest_column("age_bucket", "age_bucket"),
    state_specific_race_or_ethnicity_groupings=state_specific_query_strings.state_specific_race_or_ethnicity_groupings(),
    state_specific_supervision_type_inclusion_filter=state_specific_query_strings.state_specific_supervision_type_inclusion_filter(),
)

if __name__ == "__main__":
    with local_project_id_override(GCP_PROJECT_STAGING):
        SUPERVISION_POPULATION_BY_PRIORITIZED_RACE_AND_ETHNICITY_BY_PERIOD_VIEW_BUILDER.build_and_print()
