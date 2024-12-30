# Recidiviz - a data platform for criminal justice reform
# Copyright (C) 2024 Recidiviz, Inc.
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
"""Helper for building single observation type aggregated metrics queries."""
from recidiviz.aggregated_metrics.assignments_by_time_period_view_builder import (
    AssignmentsByTimePeriodViewBuilder,
)
from recidiviz.aggregated_metrics.metric_time_period_config import (
    MetricTimePeriodConfig,
)
from recidiviz.aggregated_metrics.models.aggregated_metric import (
    AggregatedMetric,
    AssignmentEventAggregatedMetric,
    AssignmentSpanAggregatedMetric,
    PeriodEventAggregatedMetric,
    PeriodSpanAggregatedMetric,
)
from recidiviz.aggregated_metrics.models.metric_unit_of_analysis_type import (
    MetricUnitOfAnalysis,
    MetricUnitOfAnalysisType,
)
from recidiviz.aggregated_metrics.query_building.aggregated_metric_query_utils import (
    AggregatedMetricClassType,
    metric_group_by_columns,
)
from recidiviz.calculator.query.bq_utils import (
    list_to_query_string,
    nonnull_end_date_clause,
)
from recidiviz.observations.event_observation_big_query_view_builder import (
    EventObservationBigQueryViewBuilder,
)
from recidiviz.observations.event_type import EventType
from recidiviz.observations.metric_unit_of_observation import MetricUnitOfObservation
from recidiviz.observations.observation_selector import ObservationSelector
from recidiviz.observations.observation_type_utils import ObservationTypeT
from recidiviz.observations.span_observation_big_query_view_builder import (
    SpanObservationBigQueryViewBuilder,
)
from recidiviz.observations.span_type import SpanType
from recidiviz.utils.string_formatting import fix_indent

OBSERVATIONS_CTE_NAME = "observations"
OBSERVATIONS_BY_ASSIGNMENTS_CTE_NAME = "observations_by_assignments"


def _get_referenced_attributes(
    # All of these have selectors that reference the same observation_type
    single_observation_type_metrics: list[AggregatedMetric[ObservationTypeT]],
) -> list[str]:
    referenced_attributes = set()
    for metric in single_observation_type_metrics:
        referenced_attributes.update(metric.referenced_observation_attributes())
    return sorted(referenced_attributes)


def _build_observations_query_template_for_metrics(
    observation_type: ObservationTypeT,
    # All of these have selectors that reference the same observation_type
    single_observation_type_metrics: list[AggregatedMetric[ObservationTypeT]],
) -> str:
    """Returns a query template (with a project_id format arg) that will return
    observation rows for every observation relevant to ANY of the given metrics.
    """
    if not single_observation_type_metrics:
        raise ValueError(
            f"Must provide at least one metric. Found no metrics for observation type "
            f"[{observation_type}]."
        )

    for metric in single_observation_type_metrics:
        if (
            metric_observation_type := metric.observation_selector.observation_type
        ) != observation_type:
            raise ValueError(
                f"Unexpected metric type [{metric_observation_type}] for metric "
                f"[{metric.name}]. Expected [{observation_type}]."
            )
    selectors = [
        metric.observation_selector for metric in single_observation_type_metrics
    ]

    return ObservationSelector.build_selected_observations_query_template(
        observation_type=observation_type,
        observation_selectors=selectors,
        output_attribute_columns=_get_referenced_attributes(
            single_observation_type_metrics
        ),
    )


def _aggregation_clause_for_metric(metric: AggregatedMetric) -> str:
    """Returns the aggregation logic for the given metric that is used in the SELECT
    statement of the overall query to produce the actual metric value.
    """

    if isinstance(metric, PeriodEventAggregatedMetric):
        return metric.generate_aggregation_query_fragment(
            filter_observations_by_type=False,
            read_observation_attributes_from_json=False,
            observations_cte_name=OBSERVATIONS_BY_ASSIGNMENTS_CTE_NAME,
            event_date_col=f"{OBSERVATIONS_BY_ASSIGNMENTS_CTE_NAME}.{EventObservationBigQueryViewBuilder.EVENT_DATE_OUTPUT_COL_NAME}",
        )

    if isinstance(metric, PeriodSpanAggregatedMetric):
        span_start_date_col_clause = f"""GREATEST(
            {OBSERVATIONS_BY_ASSIGNMENTS_CTE_NAME}.{SpanObservationBigQueryViewBuilder.START_DATE_OUTPUT_COL_NAME},
            {AssignmentsByTimePeriodViewBuilder.ASSIGNMENT_START_DATE_COLUMN_NAME}
        )"""

        span_end_date_col_clause = f"""LEAST(
            {nonnull_end_date_clause(f"{OBSERVATIONS_BY_ASSIGNMENTS_CTE_NAME}.{SpanObservationBigQueryViewBuilder.END_DATE_OUTPUT_COL_NAME}")},
            {AssignmentsByTimePeriodViewBuilder.ASSIGNMENT_END_DATE_EXCLUSIVE_COLUMN_NAME}
        )"""
        return metric.generate_aggregation_query_fragment(
            filter_observations_by_type=False,
            read_observation_attributes_from_json=False,
            observations_cte_name=OBSERVATIONS_BY_ASSIGNMENTS_CTE_NAME,
            period_start_date_col=MetricTimePeriodConfig.METRIC_TIME_PERIOD_START_DATE_COLUMN,
            period_end_date_col=MetricTimePeriodConfig.METRIC_TIME_PERIOD_END_DATE_EXCLUSIVE_COLUMN,
            original_span_start_date=f"{OBSERVATIONS_BY_ASSIGNMENTS_CTE_NAME}.{SpanObservationBigQueryViewBuilder.START_DATE_OUTPUT_COL_NAME}",
            span_start_date_col=span_start_date_col_clause,
            span_end_date_col=span_end_date_col_clause,
        )
    if isinstance(metric, AssignmentEventAggregatedMetric):
        raise NotImplementedError(
            "TODO(#35897): Implement aggregation clause for AssignmentEventAggregatedMetric"
        )
    if isinstance(metric, AssignmentSpanAggregatedMetric):
        raise NotImplementedError(
            "TODO(#35898): Implement aggregation clause for AssignmentSpanAggregatedMetric"
        )

    raise ValueError(f"Unexpected metric class type: [{type(metric)}]")


def _observation_to_assignment_periods_join_logic(
    *,
    metric_class: AggregatedMetricClassType,
    metric_unit_of_observation: MetricUnitOfObservation,
    assignments_by_time_period_cte_name: str,
) -> str:
    """Returns SQL for the join logic to join metric assignments rows to observation
    rows. The |assignments_by_time_period_cte_name| gives us the name of the CTE where
    assignment rows can be queried from.

    NOTE: For performance reasons, this query

    """
    observation_primary_key_columns = sorted(
        metric_unit_of_observation.primary_key_columns
    )
    shared_join_clause = "\nAND ".join(
        f"{OBSERVATIONS_CTE_NAME}.{column} = {assignments_by_time_period_cte_name}.{column}"
        for column in observation_primary_key_columns
    )

    if issubclass(metric_class, PeriodEventAggregatedMetric):
        return f"""
        {fix_indent(shared_join_clause, indent_level=8)}
        -- Include events occurring on the last date of an end-date exclusive span,
        -- but exclude events occurring on the last date of an end-date exclusive 
        -- analysis period.
        AND {OBSERVATIONS_CTE_NAME}.event_date >= {assignments_by_time_period_cte_name}.{AssignmentsByTimePeriodViewBuilder.INTERSECTION_START_DATE_COLUMN_NAME}
        AND {OBSERVATIONS_CTE_NAME}.event_date <  {assignments_by_time_period_cte_name}.{AssignmentsByTimePeriodViewBuilder.INTERSECTION_EXTENDED_END_DATE_EXCLUSIVE_NONNULL_COLUMN_NAME}
        """

    if issubclass(metric_class, PeriodSpanAggregatedMetric):
        return f"""
        {fix_indent(shared_join_clause, indent_level=8)}
        AND {OBSERVATIONS_CTE_NAME}.start_date <= {assignments_by_time_period_cte_name}.{AssignmentsByTimePeriodViewBuilder.INTERSECTION_END_DATE_EXCLUSIVE_NONNULL_COLUMN_NAME}
        AND (
            {OBSERVATIONS_CTE_NAME}.end_date IS NULL OR
            {OBSERVATIONS_CTE_NAME}.end_date > {assignments_by_time_period_cte_name}.{AssignmentsByTimePeriodViewBuilder.INTERSECTION_START_DATE_COLUMN_NAME}
        )
        """
    if issubclass(metric_class, AssignmentEventAggregatedMetric):
        raise NotImplementedError(
            "TODO(#35897): Implement JOIN logic for AssignmentEventAggregatedMetric"
        )
    if issubclass(metric_class, AssignmentSpanAggregatedMetric):
        raise NotImplementedError(
            "TODO(#35898): Implement JOIN logic for AssignmentSpanAggregatedMetric"
        )

    raise ValueError(f"Unexpected metric class type: [{metric_class}]")


def _build_observations_by_assignments_query_template(
    observation_type: ObservationTypeT,
    unit_of_analysis_type: MetricUnitOfAnalysisType,
    metric_class: AggregatedMetricClassType,
    single_observation_type_metrics: list[AggregatedMetric[ObservationTypeT]],
    assignments_by_time_period_cte_name: str,
) -> str:
    """Returns a query template (with a project_id format arg) that will return
    rows for every observation relevant to ANY of the given metrics, joined with any
    assignments that those observations should be associated with.
    """
    assignments_columns = AssignmentsByTimePeriodViewBuilder.get_output_columns(
        unit_of_analysis=MetricUnitOfAnalysis.for_type(unit_of_analysis_type),
        unit_of_observation=MetricUnitOfObservation(
            type=observation_type.unit_of_observation_type
        ),
        metric_time_period_to_assignment_join_type=metric_class.metric_time_period_to_assignment_join_type(),
    )

    observation_columns: list[str]
    if isinstance(observation_type, EventType):
        observation_columns = (
            EventObservationBigQueryViewBuilder.non_attribute_output_columns(
                observation_type.unit_of_observation_type
            )
        )
    elif isinstance(observation_type, SpanType):
        observation_columns = (
            SpanObservationBigQueryViewBuilder.non_attribute_output_columns(
                observation_type.unit_of_observation_type
            )
        )
    else:
        raise ValueError(f"Unexpected observation_type [{observation_type}]")

    observation_columns += _get_referenced_attributes(single_observation_type_metrics)

    column_strs = [
        f"{assignments_by_time_period_cte_name}.{col}" for col in assignments_columns
    ] + [
        f"{OBSERVATIONS_CTE_NAME}.{col}"
        for col in observation_columns
        if col not in assignments_columns
    ]
    columns_str = ",\n".join(column_strs)

    join_logic = _observation_to_assignment_periods_join_logic(
        metric_class=metric_class,
        assignments_by_time_period_cte_name=assignments_by_time_period_cte_name,
        metric_unit_of_observation=MetricUnitOfObservation(
            type=observation_type.unit_of_observation_type
        ),
    )
    return f"""
SELECT
{fix_indent(columns_str, indent_level=4)}
FROM 
    {assignments_by_time_period_cte_name}
JOIN 
    {OBSERVATIONS_CTE_NAME}
ON
{fix_indent(join_logic, indent_level=4)}
"""


def build_single_observation_type_aggregated_metric_query_template(
    *,
    observation_type: ObservationTypeT,
    unit_of_analysis_type: MetricUnitOfAnalysisType,
    metric_class: AggregatedMetricClassType,
    single_observation_type_metrics: list[AggregatedMetric[ObservationTypeT]],
    assignments_by_time_period_cte_name: str,
) -> str:
    """Returns a query template (with a project_id format arg) that can be used to
    calculate aggregated metrics for metrics matching a single observation type and
    metric class type.

    NOTE: For performance reasons, this query will only return rows for
    assignment / metric periods where there were non-zero observations about the unit of
    analysis. If you want to return rows for ALL assignment / metric periods, you will
    have to join the results of this query back with the assignments by time period
    table.
    """

    if not single_observation_type_metrics:
        raise ValueError(
            f"Must provide at least one metric. Found no metrics for "
            f"[{observation_type}] and [{metric_class}]"
        )

    for metric in single_observation_type_metrics:
        if not isinstance(metric, metric_class):
            raise ValueError(
                f"Found metric [{metric.name}] which has type [{type(metric)}] which "
                f"is not the expected type [{metric_class}]"
            )

        if metric.observation_type != observation_type:
            raise ValueError(
                f"Found metric [{metric.name}] which has observation_type "
                f"[{observation_type}] which is not the expected type "
                f"[{observation_type}]"
            )

    observations_cte = _build_observations_query_template_for_metrics(
        observation_type, single_observation_type_metrics
    )

    observations_by_assignments_cte = _build_observations_by_assignments_query_template(
        observation_type,
        unit_of_analysis_type,
        metric_class,
        single_observation_type_metrics,
        assignments_by_time_period_cte_name,
    )

    group_by_cols: list[str] = metric_group_by_columns(unit_of_analysis_type)
    output_column_strs = [
        *group_by_cols,
        *[
            fix_indent(_aggregation_clause_for_metric(metric), indent_level=0)
            for metric in single_observation_type_metrics
        ],
    ]
    output_columns_str = ",\n".join(output_column_strs)

    # NOTE: Changing the JOIN in this query to LEFT OUTER JOIN means that we produce
    # rows for all possible row primary key values, but it also makes the overall
    # aggregated metrics queries 2+ times slower. Instead, we join the results here
    # back to possible primary key combos in build_aggregated_metric_query_template()
    # and coalesce NULL values to zero where appropriate.
    return f"""
WITH
{OBSERVATIONS_CTE_NAME} AS (
{fix_indent(observations_cte, indent_level=4)}
),
{OBSERVATIONS_BY_ASSIGNMENTS_CTE_NAME} AS (
{fix_indent(observations_by_assignments_cte, indent_level=4)}
)
SELECT
{fix_indent(output_columns_str, indent_level=4)}
FROM {OBSERVATIONS_BY_ASSIGNMENTS_CTE_NAME}
GROUP BY {list_to_query_string(group_by_cols)}
"""
