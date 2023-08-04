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
"""Util functions to support constructing LookML fields and view fragments using AggregatedMetric objects"""

from typing import List, Optional

from more_itertools import one

from recidiviz.aggregated_metrics.models.aggregated_metric import (
    AggregatedMetric,
    AssignmentDaysToFirstEventMetric,
    AssignmentEventCountMetric,
    AssignmentSpanDaysMetric,
    DailyAvgSpanCountMetric,
    DailyAvgSpanValueMetric,
    DailyAvgTimeSinceSpanStartMetric,
    EventCountMetric,
    EventValueMetric,
    MiscAggregatedMetric,
    SumSpanDaysMetric,
)
from recidiviz.calculator.query.state.views.analyst_data.models.metric_unit_of_analysis_type import (
    MetricUnitOfAnalysis,
)
from recidiviz.looker import lookml_view_field_parameter
from recidiviz.looker.lookml_view_field import (
    DimensionLookMLViewField,
    LookMLFieldParameter,
    MeasureLookMLViewField,
    ParameterLookMLViewField,
)
from recidiviz.looker.lookml_view_field_parameter import (
    LookMLFieldType,
    LookMLSqlReferenceType,
)
from recidiviz.looker.parameterized_value import ParameterizedValue


def _generate_lookml_measure_fragment(
    metric: AggregatedMetric, days_in_period_clause: str
) -> Optional[str]:
    """
    Returns the appropriate formula for aggregated a metric over multiple time periods
    """

    if isinstance(
        metric,
        (
            AssignmentDaysToFirstEventMetric,
            AssignmentEventCountMetric,
            AssignmentSpanDaysMetric,
            EventCountMetric,
            SumSpanDaysMetric,
        ),
    ):
        return f"SUM(${{TABLE}}.{metric.name})"
    if isinstance(
        metric,
        (
            DailyAvgSpanValueMetric,
            DailyAvgTimeSinceSpanStartMetric,
            MiscAggregatedMetric,
        ),
    ):
        return (
            f"SUM(${{TABLE}}.{metric.name} * ${{TABLE}}.avg_daily_population * {days_in_period_clause}) / "
            f"SUM(IF(${{TABLE}}.{metric.name} IS NULL, 0, 1) * ${{TABLE}}.avg_daily_population * {days_in_period_clause})"
        )
    if isinstance(metric, DailyAvgSpanCountMetric):
        return (
            f"SUM(${{TABLE}}.{metric.name} * {days_in_period_clause}) / "
            f"SUM({days_in_period_clause})"
        )
    if isinstance(metric, EventValueMetric):
        return (
            f"SAFE_DIVIDE(SUM(${{TABLE}}.{metric.name} * ${{TABLE}}.{metric.event_count_metric.name}), "
            f"SUM(${{TABLE}}.{metric.event_count_metric.name}))"
        )
    return "NULL"


def _generate_lookml_measure_fragment_normalized(
    metric: AggregatedMetric,
    days_in_period_clause: str,
    allow_custom_denominator: bool,
) -> Optional[str]:
    """
    Returns the appropriate formula for aggregated a metric over multiple time periods
    and converting to a normalized rate. If `allow_custom_denominator` is true, permit
    injection of user-selected metric as denominator for DailyAvgSpanCount and EventCount metrics
    if measure_type is normalized.
    """

    custom_denominator = (
        "${metric_denominator_value}"
        if allow_custom_denominator
        else "${TABLE}.avg_daily_population"
    )

    if isinstance(
        metric,
        (
            AssignmentDaysToFirstEventMetric,
            AssignmentEventCountMetric,
            AssignmentSpanDaysMetric,
        ),
    ):
        return f"SUM(${{TABLE}}.{metric.name}) / SUM(${{TABLE}}.assignments)"
    if isinstance(metric, (DailyAvgSpanCountMetric, EventCountMetric)):
        return (
            f"SUM(SAFE_DIVIDE(${{TABLE}}.{metric.name} * {days_in_period_clause}, {custom_denominator})) / "
            f"SUM({days_in_period_clause})"
        )
    if isinstance(metric, SumSpanDaysMetric):
        return (
            f"SUM(${{TABLE}}.{metric.name}) / "
            f"SUM(${{TABLE}}.avg_daily_population * {days_in_period_clause})"
        )
    return "NULL"


def measure_for_metric(
    metric: AggregatedMetric,
    days_in_period_source: LookMLSqlReferenceType,
    param_source_view: Optional[str] = None,
    allow_custom_denominator: bool = False,
) -> MeasureLookMLViewField:
    """
    Returns a LookML measure for the specified metric, with SQL required to aggregate the metric
    using the selected `measure_type` parameter. If `param_source_view` is specified, reference the metric
    from that source view.
    """
    if days_in_period_source == LookMLSqlReferenceType.TABLE_COLUMN:
        days_in_period_clause = "${TABLE}.days_in_period"
    elif days_in_period_source == LookMLSqlReferenceType.DIMENSION:
        days_in_period_clause = "${days_in_period}"
    else:
        raise TypeError(
            f"{days_in_period_source} is an unsupported SQL reference type."
        )

    # TODO(#18172): Add the option to take a unit-level average.
    param_source_view_str = f"{param_source_view}." if param_source_view else ""
    sql = f"""{{% if {param_source_view_str}measure_type._parameter_value == "normalized" %}}
        {_generate_lookml_measure_fragment_normalized(metric, days_in_period_clause, allow_custom_denominator)}
        {{% else %}}
        {_generate_lookml_measure_fragment(metric, days_in_period_clause)}
        {{% endif %}}"""
    return MeasureLookMLViewField(
        field_name=f"{metric.name}_measure",
        parameters=[
            LookMLFieldParameter.label(metric.display_name),
            LookMLFieldParameter.description(metric.description),
            LookMLFieldParameter.type(LookMLFieldType.NUMBER),
            LookMLFieldParameter.group_label(metric.pretty_name()),
            LookMLFieldParameter.sql(sql),
        ],
    )


def get_metric_explore_parameter(
    metrics: List[AggregatedMetric],
    field_name: str = "metric_filter",
    aggregation_level: Optional[MetricUnitOfAnalysis] = None,
    default_metric: Optional[AggregatedMetric] = None,
) -> ParameterLookMLViewField:
    """
    Returns a LookML parameter for metric selection, with allowed values for all supported metrics.
    """
    additional_params = (
        [LookMLFieldParameter.group_label(aggregation_level.pretty_name)]
        if aggregation_level
        else []
    )
    default_value = metrics[0].name if default_metric is None else default_metric.name

    return ParameterLookMLViewField(
        field_name=field_name,
        parameters=[
            LookMLFieldParameter.view_label("Metric Menu"),
            *additional_params,
            LookMLFieldParameter.type(LookMLFieldType.UNQUOTED),
            *[
                LookMLFieldParameter.allowed_value(metric.display_name, metric.name)
                for metric in metrics
            ],
            LookMLFieldParameter.default_value(default_value),
        ],
    )


def get_metric_value_measure(
    view_name: str,
    metric_filter_parameter: ParameterLookMLViewField,
    aggregation_level: Optional[MetricUnitOfAnalysis] = None,
) -> MeasureLookMLViewField:
    """
    Returns a measure LookML field that uses liquid to return the metric measure selected via the metric menu filter.
    """
    view_label_parameter = one(
        p
        for p in metric_filter_parameter.parameters
        if isinstance(p, lookml_view_field_parameter.FieldParameterViewLabel)
    )

    allowed_values = [
        param.value_param for param in metric_filter_parameter.allowed_values()
    ]
    sql_value = ParameterizedValue(
        parameter_name=f"{view_name}.{metric_filter_parameter.field_name}",
        parameter_options=allowed_values,
        value_builder=lambda s: "${" + s + "_measure}",
        indentation_level=3,
    )
    additional_params = (
        [LookMLFieldParameter.group_label(aggregation_level.pretty_name)]
        if aggregation_level
        else []
    )
    return MeasureLookMLViewField(
        field_name="metric_value",
        parameters=[
            LookMLFieldParameter.description(
                f"Takes the measure value associated with the metric chosen using `{metric_filter_parameter.field_name}`"
            ),
            *additional_params,
            LookMLFieldParameter.type(LookMLFieldType.NUMBER),
            LookMLFieldParameter.view_label(view_label_parameter.text),
            LookMLFieldParameter.sql(sql_value),
        ],
    )


def get_metric_value_dimension(
    view_name: str,
    metric_filter_parameter: ParameterLookMLViewField,
    field_name: str = "metric_dimension",
    aggregation_level: Optional[MetricUnitOfAnalysis] = None,
) -> DimensionLookMLViewField:
    """
    Returns a dimension LookML field that uses liquid to return the metric dimension selected via a metric menu filter.
    """
    view_label_parameter = one(
        p
        for p in metric_filter_parameter.parameters
        if isinstance(p, lookml_view_field_parameter.FieldParameterViewLabel)
    )

    allowed_values = [
        param.value_param for param in metric_filter_parameter.allowed_values()
    ]
    sql_value = ParameterizedValue(
        parameter_name=f"{view_name}.{metric_filter_parameter.field_name}",
        parameter_options=allowed_values,
        value_builder=lambda s: "${TABLE}." + s,
        indentation_level=3,
    )
    additional_params = (
        [LookMLFieldParameter.group_label(aggregation_level.pretty_name)]
        if aggregation_level
        else []
    )
    return DimensionLookMLViewField(
        field_name=field_name,
        parameters=[
            LookMLFieldParameter.description(
                f"Takes the dimension value associated with the metric chosen using `{metric_filter_parameter.field_name}`"
            ),
            *additional_params,
            LookMLFieldParameter.type(LookMLFieldType.NUMBER),
            LookMLFieldParameter.view_label(view_label_parameter.text),
            LookMLFieldParameter.sql(sql_value),
        ],
    )
