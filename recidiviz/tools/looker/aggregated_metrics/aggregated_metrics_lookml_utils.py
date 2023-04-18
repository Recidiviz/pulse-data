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
    SumSpanDaysMetric,
)
from recidiviz.calculator.query.state.views.analyst_data.models.metric_unit_of_analysis_type import (
    MetricUnitOfAnalysis,
)
from recidiviz.looker import lookml_view_field_parameter
from recidiviz.looker.lookml_view_field import (
    LookMLFieldParameter,
    MeasureLookMLViewField,
    ParameterLookMLViewField,
)
from recidiviz.looker.lookml_view_field_parameter import (
    LookMLFieldType,
    LookMLSqlReferenceType,
)


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
    if isinstance(metric, (DailyAvgSpanValueMetric, DailyAvgTimeSinceSpanStartMetric)):
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
    metric: AggregatedMetric, days_in_period_clause: str
) -> Optional[str]:
    """
    Returns the appropriate formula for aggregated a metric over multiple time periods
    and converting to a normalized rate
    """

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
            f"(SUM(${{TABLE}}.{metric.name} * {days_in_period_clause}) / SUM(${{TABLE}}.avg_daily_population)) / "
            f"SUM({days_in_period_clause})"
        )
    if isinstance(metric, SumSpanDaysMetric):
        return (
            f"SUM(${{TABLE}}.{metric.name}) / "
            f"SUM(${{TABLE}}.avg_daily_population * {days_in_period_clause})"
        )
    return "NULL"


def measure_for_metric(
    metric: AggregatedMetric, days_in_period_source: LookMLSqlReferenceType
) -> MeasureLookMLViewField:
    """
    Returns a LookML measure for the specified metric, with SQL required to aggregate the metric
    using the selected `measure_type` parameter.
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
    sql = f"""{{% if measure_type._parameter_value == "normalized" %}}
        {_generate_lookml_measure_fragment_normalized(metric, days_in_period_clause)}
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


def get_metric_filter_parameter(
    metrics: List[AggregatedMetric],
    aggregation_level: Optional[MetricUnitOfAnalysis] = None,
) -> ParameterLookMLViewField:
    """
    Returns a LookML parameter for metric selection, with allowed values for all supported metrics.
    """
    additional_params = (
        [LookMLFieldParameter.group_label(aggregation_level.pretty_name)]
        if aggregation_level
        else []
    )
    return ParameterLookMLViewField(
        field_name="metric_filter",
        parameters=[
            LookMLFieldParameter.description(
                "Used to select one metric for a Look. Works across all levels of observation."
            ),
            LookMLFieldParameter.view_label("Metric Menu"),
            *additional_params,
            LookMLFieldParameter.type(LookMLFieldType.UNQUOTED),
            *[
                LookMLFieldParameter.allowed_value(metric.display_name, metric.name)
                for metric in metrics
            ],
            LookMLFieldParameter.default_value(metrics[0].name),
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

    metric_parameter_clauses = []
    for ix, allowed_value_param in enumerate(metric_filter_parameter.allowed_values()):
        allowed_value = allowed_value_param.value_param
        conditional_str = "elsif"
        if ix == 0:
            conditional_str = "if"
        metric_parameter_clauses.append(
            f"""{{% {conditional_str} {view_name}.{metric_filter_parameter.field_name}._parameter_value == "{allowed_value}" %}} ${{{allowed_value}_measure}}"""
        )
    metric_parameter_clauses.append("{% endif %}")

    sql_text = "\n      ".join(metric_parameter_clauses)
    additional_params = (
        [LookMLFieldParameter.group_label(aggregation_level.pretty_name)]
        if aggregation_level
        else []
    )
    return MeasureLookMLViewField(
        field_name="metric_value",
        parameters=[
            LookMLFieldParameter.description(
                f"Takes the value associated with the metric chosen using `{metric_filter_parameter.field_name}`"
            ),
            *additional_params,
            LookMLFieldParameter.type(LookMLFieldType.NUMBER),
            LookMLFieldParameter.view_label(view_label_parameter.text),
            LookMLFieldParameter.sql(sql_text),
        ],
    )
