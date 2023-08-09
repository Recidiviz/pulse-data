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
    AssignmentEventAggregatedMetric,
    AssignmentSpanAggregatedMetric,
    MetricConditionsMixin,
    MiscAggregatedMetric,
    PeriodEventAggregatedMetric,
    PeriodSpanAggregatedMetric,
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
    metrics_query_str = ",\n    ".join([metric.name for metric in included_metrics])
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
{population.get_population_query()}
```

#### Aggregation Level Definition: {level_name.title()}

Source table:
```
{aggregation_level.client_assignment_query}
```

Primary key columns: `{aggregation_level.get_primary_key_columns_query_string()}`

Static attribute columns: `{aggregation_level.get_static_attribute_columns_query_string()}`

### Metrics:
|	Metric	|	Description	|   Metric Type    |   Conditions   |
|	--------------------	|	--------------------	|	--------------------	|	--------------------	|
{view_description_metrics_str}
    """

    # determine which metric source tables to include
    included_metric_types = {type(metric) for metric in included_metrics}

    # generate FROM clause
    from_clause = ""
    metric_class_to_table_mapping = {
        PeriodSpanAggregatedMetric: "period_span",
        PeriodEventAggregatedMetric: "period_event",
        AssignmentSpanAggregatedMetric: "assignment_span",
        AssignmentEventAggregatedMetric: "assignment_event",
        MiscAggregatedMetric: "misc",
    }

    for metric_class, table_name in metric_class_to_table_mapping.items():
        # Check if there is a metric of the given class. If so, we need to add the
        # table to the FROM clause.
        if any(
            issubclass(metric_type, metric_class)
            for metric_type in included_metric_types
        ):
            # For misc metrics, check if the view builder exists for the population,
            # and if not, don't add the table to the FROM clause.
            if metric_class == MiscAggregatedMetric:
                misc_metrics_view_builder = (
                    generate_misc_aggregated_metrics_view_builder(
                        aggregation_level=aggregation_level,
                        population=population,
                        metrics=[
                            m
                            for m in included_metrics
                            if isinstance(m, MiscAggregatedMetric)
                        ],
                    )
                )
                if not misc_metrics_view_builder:
                    continue

            # Add table to the FROM clause
            table_ref = f"`{{project_id}}.aggregated_metrics.{population_name}_{level_name}_{table_name}_aggregated_metrics` {table_name}"
            if not from_clause:
                join_type = "FROM"
                join_condition = ""
            else:
                join_type = "LEFT JOIN"
                join_condition = f"USING ({aggregation_level.get_primary_key_columns_query_string()}, start_date, end_date, period)"
            from_clause += f"""
{join_type}
    {table_ref}
{join_condition}
"""

    # verify from_clause is hydrated
    if not from_clause:
        raise ValueError(f"No metrics found for {population_name} metrics")

    # construct query template
    query_template = f"""
SELECT
    {aggregation_level.get_index_columns_query_string(prefix="period_span")},
    start_date,
    end_date,
    period,
    {metrics_query_str},{from_clause}
"""

    return SimpleBigQueryViewBuilder(
        dataset_id=AGGREGATED_METRICS_DATASET_ID,
        view_id=view_id,
        view_query_template=query_template,
        description=view_description,
        bq_description=view_description_header,
        should_materialize=True,
        clustering_fields=aggregation_level.primary_key_columns,
    )
