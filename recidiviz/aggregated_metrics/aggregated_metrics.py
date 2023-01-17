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
"""Joins together all aggregated metric views for the specified population and aggregation level"""
from recidiviz.aggregated_metrics.dataset_config import AGGREGATED_METRICS_DATASET_ID
from recidiviz.aggregated_metrics.misc_aggregated_metrics import (
    generate_misc_aggregated_metrics_view_builder,
)
from recidiviz.aggregated_metrics.models.metric_aggregation_level_type import (
    MetricAggregationLevel,
)
from recidiviz.aggregated_metrics.models.metric_population_type import MetricPopulation
from recidiviz.big_query.big_query_view import SimpleBigQueryViewBuilder


def generate_aggregated_metrics_view_builder(
    aggregation_level: MetricAggregationLevel,
    population: MetricPopulation,
) -> SimpleBigQueryViewBuilder:
    """
    Returns a SimpleBigQueryViewBuilder that joins together all metric views into a
    single materialized view for the specified aggregation level and population.
    """
    level_name = aggregation_level.level_name_short
    population_name = population.population_name_short
    view_id = f"{population_name}_{level_name}_aggregated_metrics"
    view_description = f"""
    All metrics for the {population_name} population disaggregated by {level_name}.
    """

    query_template = f"""
    SELECT * EXCEPT (assignments),
        COALESCE(assignments, 0) AS assignments,
    FROM
        `{{project_id}}.{{aggregated_metrics_dataset}}.{population_name}_{level_name}_period_span_aggregated_metrics` period_span
    LEFT JOIN
        `{{project_id}}.{{aggregated_metrics_dataset}}.{population_name}_{level_name}_period_event_aggregated_metrics` period_event
    USING (
        {aggregation_level.get_index_columns_query_string()},
        start_date, end_date, period
    )
    LEFT JOIN
        `{{project_id}}.{{aggregated_metrics_dataset}}.{population_name}_{level_name}_assignment_span_aggregated_metrics` assignment_span
    USING (
        {aggregation_level.get_index_columns_query_string()},
        start_date, end_date, period
    )
    LEFT JOIN
        `{{project_id}}.{{aggregated_metrics_dataset}}.{population_name}_{level_name}_assignment_event_aggregated_metrics` assignment_event
    USING (
        {aggregation_level.get_index_columns_query_string()},
        start_date, end_date, period
    )
"""

    misc_metrics_view_builder = generate_misc_aggregated_metrics_view_builder(
        aggregation_level=aggregation_level,
        population=population,
    )
    if misc_metrics_view_builder:
        # Join to miscellaneous metrics view if view exists for population and aggregation level
        query_template += f"""
        LEFT JOIN
            `{{project_id}}.{{aggregated_metrics_dataset}}.{misc_metrics_view_builder.view_id}` misc
        USING (
            {aggregation_level.get_index_columns_query_string()},
            start_date, end_date, period
        )
    """

    return SimpleBigQueryViewBuilder(
        dataset_id=AGGREGATED_METRICS_DATASET_ID,
        view_id=view_id,
        view_query_template=query_template,
        description=view_description,
        aggregated_metrics_dataset=AGGREGATED_METRICS_DATASET_ID,
        should_materialize=True,
        clustering_fields=aggregation_level.index_columns,
    )
