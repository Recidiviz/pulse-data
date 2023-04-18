# Recidiviz - a data platform for criminal justice reform
# Copyright (C) 2023 Recidiviz, Inc.
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
from typing import List

from recidiviz.aggregated_metrics.dataset_config import AGGREGATED_METRICS_DATASET_ID
from recidiviz.aggregated_metrics.misc_aggregated_metrics import (
    generate_misc_aggregated_metrics_view_builder,
)
from recidiviz.aggregated_metrics.models.aggregated_metric import (
    AggregatedMetric,
    MetricConditionsMixin,
    MiscAggregatedMetric,
)
from recidiviz.big_query.big_query_view import SimpleBigQueryViewBuilder
from recidiviz.calculator.query.state.views.analyst_data.models.metric_population_type import (
    MetricPopulation,
)
from recidiviz.calculator.query.state.views.analyst_data.models.metric_unit_of_analysis_type import (
    MetricUnitOfAnalysis,
)


def generate_aggregated_metrics_view_builder(
    aggregation_level: MetricUnitOfAnalysis,
    population: MetricPopulation,
    metrics: List[AggregatedMetric],
) -> SimpleBigQueryViewBuilder:
    """
    Returns a SimpleBigQueryViewBuilder that joins together all metric views into a
    single materialized view for the specified aggregation level and population.
    """
    level_name = aggregation_level.level_name_short
    population_name = population.population_name_short
    view_id = f"{population_name}_{level_name}_aggregated_metrics"
    included_metrics = [
        metric
        for metric in metrics
        # Only display MiscAggregatedMetrics that are relevant to the population and aggregation level
        if not isinstance(metric, MiscAggregatedMetric)
        or (
            population.population_type in metric.populations
            and aggregation_level.level_type in metric.aggregation_levels
        )
    ]
    metrics_query_str = ", ".join([metric.name for metric in included_metrics])
    view_description_metrics = [
        f"|{metric.display_name} (`{metric.name}`)|{metric.description}|{metric.pretty_name()}|"
        f"`{metric.get_metric_conditions_string_no_newline() if isinstance(metric, MetricConditionsMixin) else 'N/A'}`|"
        for metric in included_metrics
    ]
    view_description_metrics_str = "\n".join(view_description_metrics)
    view_description_header = f"All metrics for the {population_name} population disaggregated by {level_name}."
    view_description = f"""
{view_description_header}

#### Population Definition: {population_name.title()}

```
{population.get_conditions_query_string()}
```

#### Aggregation Level Definition: {level_name.title()}

Source table: 
```
{aggregation_level.client_assignment_query}
```

Primary key columns: `{aggregation_level.get_primary_key_columns_query_string()}`

Additional attribute columns: `{aggregation_level.get_attribute_columns_query_string()}`

### Metrics:
|	Metric	|	Description	|   Metric Type    |   Conditions   |
|	--------------------	|	--------------------	|	--------------------	|	--------------------	|
{view_description_metrics_str}
    """

    query_template = f"""
    SELECT 
        {aggregation_level.get_index_columns_query_string(prefix="period_span")},
        start_date,
        end_date,
        period,
        COALESCE(assignments, 0) AS assignments,
        {metrics_query_str},
    FROM
        `{{project_id}}.{{aggregated_metrics_dataset}}.{population_name}_{level_name}_period_span_aggregated_metrics` period_span
    LEFT JOIN
        `{{project_id}}.{{aggregated_metrics_dataset}}.{population_name}_{level_name}_period_event_aggregated_metrics` period_event
    USING (
        {aggregation_level.get_primary_key_columns_query_string()},
        start_date, end_date, period
    )
    LEFT JOIN
        `{{project_id}}.{{aggregated_metrics_dataset}}.{population_name}_{level_name}_assignment_span_aggregated_metrics` assignment_span
    USING (
        {aggregation_level.get_primary_key_columns_query_string()},
        start_date, end_date, period
    )
    LEFT JOIN
        `{{project_id}}.{{aggregated_metrics_dataset}}.{population_name}_{level_name}_assignment_event_aggregated_metrics` assignment_event
    USING (
        {aggregation_level.get_primary_key_columns_query_string()},
        start_date, end_date, period
    )
"""

    misc_metrics_view_builder = generate_misc_aggregated_metrics_view_builder(
        aggregation_level=aggregation_level,
        population=population,
        metrics=[m for m in included_metrics if isinstance(m, MiscAggregatedMetric)],
    )
    if misc_metrics_view_builder:
        # Join to miscellaneous metrics view if view exists for population and aggregation level
        query_template += f"""
        LEFT JOIN
            `{{project_id}}.{{aggregated_metrics_dataset}}.{misc_metrics_view_builder.view_id}` misc
        USING (
            {aggregation_level.get_primary_key_columns_query_string()},
            start_date, end_date, period
        )
    """

    return SimpleBigQueryViewBuilder(
        dataset_id=AGGREGATED_METRICS_DATASET_ID,
        view_id=view_id,
        view_query_template=query_template,
        description=view_description,
        bq_description=view_description_header,
        aggregated_metrics_dataset=AGGREGATED_METRICS_DATASET_ID,
        should_materialize=True,
        clustering_fields=aggregation_level.primary_key_columns,
    )
