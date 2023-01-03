# Recidiviz - a data platform for criminal justice reform
# Copyright (C) 2022 Recidiviz, Inc.
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
"""Creates view builders that generate SQL views calculating period-span metrics"""
from typing import Dict, List, Optional, Tuple

from recidiviz.aggregated_metrics.dataset_config import AGGREGATED_METRICS_DATASET_ID
from recidiviz.aggregated_metrics.models.metric_aggregation_level_type import (
    MetricAggregationLevel,
    MetricAggregationLevelType,
)
from recidiviz.aggregated_metrics.models.metric_population_type import (
    MetricPopulation,
    MetricPopulationType,
)
from recidiviz.big_query.big_query_view import SimpleBigQueryViewBuilder
from recidiviz.calculator.query.state.dataset_config import ANALYST_VIEWS_DATASET

# List of [MetricPopulationType, MetricAggregationLevelType] tuples that are supported by
# `generate_misc_aggregated_metrics_view_builder` function.
_MISC_METRICS_SUPPORTED_POPULATIONS_AGGREGATION_LEVELS: List[
    Tuple[MetricPopulationType, MetricAggregationLevelType]
] = [
    (MetricPopulationType.SUPERVISION, MetricAggregationLevelType.SUPERVISION_OFFICER),
    (MetricPopulationType.SUPERVISION, MetricAggregationLevelType.SUPERVISION_OFFICE),
    (MetricPopulationType.SUPERVISION, MetricAggregationLevelType.SUPERVISION_DISTRICT),
    (MetricPopulationType.SUPERVISION, MetricAggregationLevelType.STATE_CODE),
]


def _query_template_and_format_args(
    aggregation_level: MetricAggregationLevel,
    population: MetricPopulation,
) -> Tuple[str, Dict[str, str]]:
    """Returns the appropriate query template (and associated dataset keyword args for
    that template) for the provided population and aggregation level.
    """
    if population.population_type == MetricPopulationType.SUPERVISION:
        if (
            aggregation_level.level_type
            == MetricAggregationLevelType.SUPERVISION_OFFICER
        ):

            query_template = f"""
SELECT
    {aggregation_level.get_index_columns_query_string()},
    period,
    population_start_date AS start_date,
    population_end_date AS end_date,
    district AS primary_district,
    office AS primary_office,
FROM
    `{{project_id}}.{{aggregated_metrics_dataset}}.metric_time_periods_materialized` a
INNER JOIN 
    `{{project_id}}.{{analyst_views_dataset}}.supervision_officer_primary_office_materialized` b
ON
    b.date = a.population_start_date
"""
            return query_template, {
                "aggregated_metrics_dataset": AGGREGATED_METRICS_DATASET_ID,
                "analyst_views_dataset": ANALYST_VIEWS_DATASET,
            }
        if aggregation_level.level_type in [
            MetricAggregationLevelType.SUPERVISION_OFFICE,
            MetricAggregationLevelType.SUPERVISION_DISTRICT,
            MetricAggregationLevelType.STATE_CODE,
        ]:
            query_template = f"""
SELECT
    {aggregation_level.get_index_columns_query_string()},
    period,
    start_date,
    end_date,
    AVG(avg_daily_population) AS avg_daily_caseload_officer,
    AVG(assignments) AS avg_assignments_officer,
FROM (
    SELECT 
        *, 
        primary_district AS district, 
        primary_office AS office
    FROM
        `{{project_id}}.{{aggregated_metrics_dataset}}.supervision_officer_aggregated_metrics_materialized`
)
GROUP BY
    {aggregation_level.get_index_columns_query_string()},
    period, start_date, end_date       
"""
            return query_template, {
                "aggregated_metrics_dataset": AGGREGATED_METRICS_DATASET_ID
            }

    raise ValueError(
        f"Unexpected population_type [{population.population_type}] and "
        f"aggregation_level type [{aggregation_level.level_type}] when generating misc "
        f"metrics."
    )


def generate_misc_aggregated_metrics_view_builder(
    aggregation_level: MetricAggregationLevel,
    population: MetricPopulation,
) -> Optional[SimpleBigQueryViewBuilder]:
    """
    Returns a SimpleBigQueryViewBuilder that calculates miscellaneous metrics for the specified
    aggregation level and population, if one exists.
    """
    if (
        population.population_type,
        aggregation_level.level_type,
    ) not in _MISC_METRICS_SUPPORTED_POPULATIONS_AGGREGATION_LEVELS:
        return None

    query_template, dataset_kwargs = _query_template_and_format_args(
        aggregation_level, population
    )

    view_id = f"{population.population_name_short}_{aggregation_level.level_name_short}_misc_aggregated_metrics"
    view_description = f"""
    Metrics for the {population.population_name_short} population calculated using 
    ad-hoc logic, disaggregated by {aggregation_level.level_name_short}.

    All end_dates are exclusive, i.e. the metric is for the range [start_date, end_date).
    """

    return SimpleBigQueryViewBuilder(
        dataset_id=AGGREGATED_METRICS_DATASET_ID,
        view_id=view_id,
        view_query_template=query_template,
        description=view_description,
        should_materialize=False,
        clustering_fields=aggregation_level.index_columns,
        # We set these values so that mypy knows they are not in the dataset_kwargs
        materialized_address_override=None,
        should_deploy_predicate=None,
        projects_to_deploy=None,
        **dataset_kwargs,
    )
