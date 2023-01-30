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
"""A script for generating a copy-paste-able LookML view file containing measures for
all configured metrics associated with the specified population and aggregation level.

To print the contents of a lookml view file for the desired population and aggregation level, run:
    python -m recidiviz.tools.looker.aggregated_metrics_lookml_generator \
       --population [POPULATION] --aggregation_level [AGGREGATION_LEVEL]

To print the view contents in the console, add parameter:
    --print_view True

To save view file to a directory, add parameter:
    --save_view_to_dir [PATH]
"""
import argparse
from pathlib import Path
from typing import List, Optional

from more_itertools import one

from recidiviz.aggregated_metrics.aggregated_metric_view_collector import (
    METRICS_BY_POPULATION_TYPE,
)
from recidiviz.aggregated_metrics.aggregated_metrics import (
    generate_aggregated_metrics_view_builder,
)
from recidiviz.aggregated_metrics.models.aggregated_metric import (
    AggregatedMetric,
    AssignmentAvgSpanValueMetric,
    AssignmentDaysToFirstEventMetric,
    AssignmentEventCountMetric,
    AssignmentSpanDaysMetric,
    DailyAvgSpanCountMetric,
    DailyAvgSpanValueMetric,
    DailyAvgTimeSinceSpanStartMetric,
    EventCountMetric,
    SumSpanDaysMetric,
)
from recidiviz.aggregated_metrics.models.metric_aggregation_level_type import (
    METRIC_AGGREGATION_LEVELS_BY_TYPE,
    MetricAggregationLevel,
    MetricAggregationLevelType,
)
from recidiviz.aggregated_metrics.models.metric_population_type import (
    METRIC_POPULATIONS_BY_TYPE,
    MetricPopulation,
    MetricPopulationType,
)
from recidiviz.looker import lookml_view_field_parameter
from recidiviz.looker.lookml_view import LookMLView
from recidiviz.looker.lookml_view_field import (
    DimensionLookMLViewField,
    LookMLFieldParameter,
    MeasureLookMLViewField,
    ParameterLookMLViewField,
)
from recidiviz.looker.lookml_view_field_parameter import LookMLFieldType
from recidiviz.looker.lookml_view_source_table import LookMLViewSourceTable
from recidiviz.utils.params import str_to_bool


def _generate_lookml_measure_fragment(metric: AggregatedMetric) -> Optional[str]:
    """
    Returns the appropriate formula for aggregated a metric over multiple time periods
    """
    if isinstance(
        metric,
        (
            SumSpanDaysMetric,
            AssignmentSpanDaysMetric,
            EventCountMetric,
            AssignmentDaysToFirstEventMetric,
            AssignmentEventCountMetric,
        ),
    ):
        return f"SUM(${{TABLE}}.{metric.name})"
    if isinstance(metric, (DailyAvgSpanValueMetric, DailyAvgTimeSinceSpanStartMetric)):
        return (
            f"SUM(${{TABLE}}.{metric.name} * ${{TABLE}}.avg_daily_population * ${{days_in_period}}) / "
            f"SUM(${{TABLE}}.avg_daily_population * ${{days_in_period}})"
        )
    if isinstance(metric, DailyAvgSpanCountMetric):
        return (
            f"SUM(${{TABLE}}.{metric.name} * ${{days_in_period}}) / "
            f"SUM(${{days_in_period}})"
        )
    if isinstance(metric, AssignmentAvgSpanValueMetric):
        return (
            f"SUM(${{TABLE}}.{metric.name} * ${{TABLE}}.assignments) / "
            f"SUM(${{TABLE}}.assignments)"
        )
    return "NULL"
    # TODO(#17996): Add an event_count_metric param to the EventValueMetric


def _generate_lookml_measure_fragment_normalized(
    metric: AggregatedMetric,
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
            f"(SUM(${{TABLE}}.{metric.name} * ${{days_in_period}}) / SUM(${{TABLE}}.avg_daily_population)) / "
            f"SUM(${{days_in_period}})"
        )
    if isinstance(metric, SumSpanDaysMetric):
        return (
            f"SUM(${{TABLE}}.{metric.name}) / "
            f"SUM(${{TABLE}}.avg_daily_population * ${{days_in_period}})"
        )
    # TODO(#17996): Add an event_count_metric param to the EventValueMetric
    return "NULL"


def measure_for_metric(metric: AggregatedMetric) -> MeasureLookMLViewField:
    # TODO(#18172): Add the option to take a unit-level average.
    sql = f"""{{% if measure_type._parameter_value == "normalized" %}}
        {_generate_lookml_measure_fragment_normalized(metric)}
        {{% else %}}
        {_generate_lookml_measure_fragment(metric)}
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
    aggregation_level: MetricAggregationLevel,
) -> ParameterLookMLViewField:
    return ParameterLookMLViewField(
        field_name="metric_filter",
        parameters=[
            LookMLFieldParameter.description(
                "Used to select one metric for a Look. Works across all levels of observation."
            ),
            LookMLFieldParameter.view_label("Metric Menu"),
            LookMLFieldParameter.group_label(
                aggregation_level.level_name_short.replace("_", " ").title()
            ),
            LookMLFieldParameter.type(LookMLFieldType.UNQUOTED),
            *[
                LookMLFieldParameter.allowed_value(metric.display_name, metric.name)
                for metric in metrics
            ],
        ],
    )


def get_metric_value_measure(
    view_name: str,
    metric_filter_parameter: ParameterLookMLViewField,
    aggregation_level: MetricAggregationLevel,
) -> MeasureLookMLViewField:
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
    return MeasureLookMLViewField(
        field_name="metric_value",
        parameters=[
            LookMLFieldParameter.description(
                f"Takes the value associated with the metric chosen using `{metric_filter_parameter.field_name}`"
            ),
            LookMLFieldParameter.group_label(
                aggregation_level.level_name_short.replace("_", " ").title()
            ),
            LookMLFieldParameter.type(LookMLFieldType.NUMBER),
            LookMLFieldParameter.view_label(view_label_parameter.text),
            LookMLFieldParameter.sql(sql_text),
        ],
    )


def get_lookml_view_for_metrics(
    population: MetricPopulation,
    aggregation_level: MetricAggregationLevel,
    metrics: List[AggregatedMetric],
) -> LookMLView:
    """Generates LookML view string for the specified population, aggregation level, and metrics"""
    bq_view_builder = generate_aggregated_metrics_view_builder(
        aggregation_level,
        population,
    )
    view_name = bq_view_builder.view_id

    primary_key_col_dimensions = [
        DimensionLookMLViewField.for_column(col)
        for col in aggregation_level.index_columns
    ]
    time_dimensions_view_label = "Time"
    date_dimensions = [
        DimensionLookMLViewField.for_column(
            date_field, field_type=LookMLFieldType.DATE
        ).extend(
            additional_parameters=[
                LookMLFieldParameter.view_label(time_dimensions_view_label)
            ]
        )
        for date_field in ["start_date", "end_date"]
    ]
    time_dimensions = [
        *date_dimensions,
        DimensionLookMLViewField.for_column("period").extend(
            additional_parameters=[
                LookMLFieldParameter.view_label(time_dimensions_view_label),
            ]
        ),
        DimensionLookMLViewField.for_days_in_period(
            "start_date", "end_date", time_dimensions_view_label
        ),
        MeasureLookMLViewField(
            field_name="days_in_period_avg",
            parameters=[
                LookMLFieldParameter.description("Average days in period"),
                LookMLFieldParameter.type(LookMLFieldType.NUMBER),
                LookMLFieldParameter.view_label(time_dimensions_view_label),
                LookMLFieldParameter.sql("AVG(${days_in_period})"),
            ],
        ),
    ]

    measure_type_parameter = ParameterLookMLViewField(
        field_name="measure_type",
        parameters=[
            LookMLFieldParameter.type(LookMLFieldType.UNQUOTED),
            LookMLFieldParameter.description(
                "Used to select whether metric should be presented as a raw value or a normalized rate"
            ),
            LookMLFieldParameter.view_label("Metric Menu"),
            LookMLFieldParameter.group_label(
                aggregation_level.level_name_short.replace("_", " ").title()
            ),
            LookMLFieldParameter.allowed_value("Normalized", "normalized"),
            LookMLFieldParameter.allowed_value("Value", "value"),
            LookMLFieldParameter.default_value("value"),
        ],
    )
    metric_measures = [measure_for_metric(metric) for metric in metrics]
    metric_filter_parameter = get_metric_filter_parameter(metrics, aggregation_level)
    metric_value_measure = get_metric_value_measure(
        view_name, metric_filter_parameter, aggregation_level
    )
    return LookMLView(
        view_name=view_name,
        # `table_for_query` returns materialized table address by default
        table=LookMLViewSourceTable.sql_table_address(bq_view_builder.table_for_query),
        fields=[
            *primary_key_col_dimensions,
            *time_dimensions,
            measure_type_parameter,
            *metric_measures,
            metric_filter_parameter,
            metric_value_measure,
        ],
    )


def parse_arguments() -> argparse.Namespace:
    """Parses the required arguments."""
    parser = argparse.ArgumentParser()

    parser.add_argument(
        "--population",
        dest="population",
        help="Name of the population enum used to generate aggregated metrics.",
        type=MetricPopulationType,
        choices=list(MetricPopulationType),
        default=MetricPopulationType.SUPERVISION,
        required=True,
    )

    parser.add_argument(
        "--aggregation_level",
        dest="aggregation_level",
        help="Name of the aggregation level enum used to generate aggregated metrics.",
        type=MetricAggregationLevelType,
        choices=list(MetricAggregationLevelType),
        default=MetricAggregationLevelType.STATE_CODE,
        required=True,
    )

    parser.add_argument(
        "--print_view",
        dest="print_view",
        help="Indicates whether to print view contents in terminal",
        type=str_to_bool,
        default=False,
        required=False,
    )

    parser.add_argument(
        "--save_view_to_dir",
        dest="save_dir",
        help="Specifies name of directory where to save view file",
        type=str,
        required=False,
    )

    return parser.parse_args()


def main(
    population_type: MetricPopulationType,
    aggregation_level_type: MetricAggregationLevelType,
    print_view: bool,
    output_directory: Optional[str],
) -> None:
    lookml_view = get_lookml_view_for_metrics(
        population=METRIC_POPULATIONS_BY_TYPE[population_type],
        aggregation_level=METRIC_AGGREGATION_LEVELS_BY_TYPE[aggregation_level_type],
        metrics=METRICS_BY_POPULATION_TYPE[population_type],
    )

    if print_view:
        print(lookml_view.build())
    if output_directory:
        # if directory doesn't already exist, create
        Path(output_directory).mkdir(parents=True, exist_ok=True)
        lookml_view.write(output_directory, source_script_path=__file__)


if __name__ == "__main__":
    args = parse_arguments()
    main(args.population, args.aggregation_level, args.print_view, args.save_dir)
