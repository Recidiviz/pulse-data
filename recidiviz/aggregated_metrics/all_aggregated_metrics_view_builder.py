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
"""Joins together all aggregated metric views for the specified population and unit of analysis"""
from typing import List, Optional

from recidiviz.aggregated_metrics.assignment_sessions_view_builder import (
    get_assignment_query_for_unit_of_analysis,
)
from recidiviz.aggregated_metrics.assignment_sessions_view_collector import (
    get_standard_population_selector_for_unit_of_observation,
)
from recidiviz.aggregated_metrics.dataset_config import AGGREGATED_METRICS_DATASET_ID
from recidiviz.aggregated_metrics.models.aggregated_metric import (
    AggregatedMetric,
    AssignmentEventAggregatedMetric,
    AssignmentSpanAggregatedMetric,
    PeriodEventAggregatedMetric,
    PeriodSpanAggregatedMetric,
)
from recidiviz.aggregated_metrics.models.metric_population_type import (
    MetricPopulationType,
)
from recidiviz.aggregated_metrics.models.metric_unit_of_analysis_type import (
    MetricUnitOfAnalysis,
    get_static_attributes_query_for_unit_of_analysis,
)
from recidiviz.big_query.big_query_view import SimpleBigQueryViewBuilder
from recidiviz.observations.metric_unit_of_observation_type import (
    MetricUnitOfObservationType,
)


def generate_all_aggregated_metrics_view_builder(
    unit_of_analysis: MetricUnitOfAnalysis,
    population_type: MetricPopulationType,
    metrics: List[AggregatedMetric],
    dataset_id_override: Optional[str],
    collection_tag: str | None,
    disaggregate_by_observation_attributes: list[str] | None,
) -> SimpleBigQueryViewBuilder:
    """
    Returns a SimpleBigQueryViewBuilder that joins together all metric views for
    individual metric class types into a single materialized view for the specified unit
    of analysis and population.
    """
    unit_of_analysis_name = unit_of_analysis.type.short_name
    population_name = population_type.population_name_short
    unit_of_observation_type = MetricUnitOfObservationType.PERSON_ID

    population_selector = get_standard_population_selector_for_unit_of_observation(
        population_type=population_type,
        unit_of_observation_type=unit_of_observation_type,
    )

    if not population_selector:
        raise ValueError(
            f"Cannot build an aggregated_metrics view for [{population_type}] and "
            f"[{unit_of_observation_type}]"
        )

    person_population_query = population_selector.generate_span_selector_query()
    collection_tag_part = "" if not collection_tag else f"{collection_tag}__"
    view_id = f"{collection_tag_part}{population_name}_{unit_of_analysis_name}_aggregated_metrics"

    metrics_query_str = ",\n    ".join([metric.name for metric in metrics])
    view_description_metrics = [
        f"|{metric.display_name} (`{metric.name}`)|{metric.description}|{metric.pretty_name()}|"
        f"`{metric.get_observation_conditions_string_no_newline(filter_by_observation_type=True, read_observation_attributes_from_json=True)}`|"
        for metric in metrics
    ]
    view_description_metrics_str = "\n".join(view_description_metrics)
    view_description_header = f"All metrics for the {population_name} population disaggregated by {unit_of_analysis_name}."
    view_description = f"""
{view_description_header}

#### Population Definition: {population_name.title()}
Query for person unit of observation:
```
{person_population_query}
```

#### Unit Of Analysis Definition: {unit_of_analysis_name.title()}

Source table:
```
{get_assignment_query_for_unit_of_analysis(unit_of_analysis.type, unit_of_observation_type)}
```

Primary key columns: `{unit_of_analysis.get_primary_key_columns_query_string()}`

Static attribute columns: `{unit_of_analysis.get_static_attribute_columns_query_string()}`

### Metrics:
|	Metric	|	Description	|   Metric Type    |   Conditions   |
|	--------------------	|	--------------------	|	--------------------	|	--------------------	|
{view_description_metrics_str}
    """

    # determine which metric source tables to include
    included_metric_types = {type(metric) for metric in metrics}

    # generate FROM clause
    from_clause = ""
    metric_classes = [
        PeriodSpanAggregatedMetric,
        PeriodEventAggregatedMetric,
        AssignmentSpanAggregatedMetric,
        AssignmentEventAggregatedMetric,
    ]

    metric_class: type[PeriodSpanAggregatedMetric] | type[
        PeriodEventAggregatedMetric
    ] | type[AssignmentSpanAggregatedMetric] | type[AssignmentEventAggregatedMetric]
    for metric_class in metric_classes:
        # Check if there is a metric of the given class. If so, we need to add the
        # table to the FROM clause.
        if any(
            issubclass(metric_type, metric_class)
            for metric_type in included_metric_types
        ):

            dataset_id = (
                dataset_id_override
                if dataset_id_override
                else AGGREGATED_METRICS_DATASET_ID
            )

            table_name = metric_class.metric_class_name_lower()

            # Add table to the FROM clause
            table_ref = f"`{{project_id}}.{dataset_id}.{collection_tag_part}{population_name}_{unit_of_analysis_name}_{table_name}_aggregated_metrics_materialized` {table_name}"
            if not from_clause:
                join_type = "FROM"
                join_condition = ""
            else:
                join_type = "LEFT JOIN"
                join_condition = f"USING ({unit_of_analysis.get_primary_key_columns_query_string()}, start_date, end_date, period)"
            from_clause += f"""
{join_type}
    {table_ref}
{join_condition}
"""
    static_attributes_query = get_static_attributes_query_for_unit_of_analysis(
        unit_of_analysis.type
    )
    if static_attributes_query:
        from_clause += f"""
LEFT JOIN
    ({get_static_attributes_query_for_unit_of_analysis(unit_of_analysis.type)})
USING
    ({unit_of_analysis.get_primary_key_columns_query_string()})
"""

    # verify from_clause is hydrated
    if not from_clause:
        raise ValueError(f"No metrics found for {population_name} metrics")

    # construct query template
    query_template = f"""
SELECT
    {unit_of_analysis.get_index_columns_query_string()},{", ".join(disaggregate_by_observation_attributes) + "," if disaggregate_by_observation_attributes else ""}
    start_date,
    end_date,
    period,
    {metrics_query_str},{from_clause}
"""

    return SimpleBigQueryViewBuilder(
        dataset_id=dataset_id_override or AGGREGATED_METRICS_DATASET_ID,
        view_id=view_id,
        view_query_template=query_template,
        description=view_description,
        bq_description=view_description_header,
        should_materialize=True,
        clustering_fields=unit_of_analysis.primary_key_columns,
    )
