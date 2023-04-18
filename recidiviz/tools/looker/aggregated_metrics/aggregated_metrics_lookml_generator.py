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
from typing import List, Optional

from recidiviz.aggregated_metrics.aggregated_metric_view_collector import (
    METRICS_BY_POPULATION_TYPE,
)
from recidiviz.aggregated_metrics.aggregated_metrics import (
    generate_aggregated_metrics_view_builder,
)
from recidiviz.aggregated_metrics.models.aggregated_metric import AggregatedMetric
from recidiviz.calculator.query.state.views.analyst_data.models.metric_population_type import (
    METRIC_POPULATIONS_BY_TYPE,
    MetricPopulation,
    MetricPopulationType,
)
from recidiviz.calculator.query.state.views.analyst_data.models.metric_unit_of_analysis_type import (
    METRIC_UNITS_OF_ANALYSIS_BY_TYPE,
    MetricUnitOfAnalysis,
    MetricUnitOfAnalysisType,
)
from recidiviz.looker.lookml_view import LookMLView
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
from recidiviz.looker.lookml_view_source_table import LookMLViewSourceTable
from recidiviz.tools.looker.aggregated_metrics.aggregated_metrics_lookml_utils import (
    get_metric_filter_parameter,
    get_metric_value_measure,
    measure_for_metric,
)
from recidiviz.utils.params import str_to_bool


def get_lookml_view_for_metrics(
    population: MetricPopulation,
    aggregation_level: MetricUnitOfAnalysis,
    metrics: List[AggregatedMetric],
) -> LookMLView:
    """Generates LookML view string for the specified population, aggregation level, and metrics"""
    bq_view_builder = generate_aggregated_metrics_view_builder(
        aggregation_level, population, metrics
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
            LookMLFieldParameter.group_label(aggregation_level.pretty_name),
            LookMLFieldParameter.allowed_value("Normalized", "normalized"),
            LookMLFieldParameter.allowed_value("Value", "value"),
            LookMLFieldParameter.default_value("value"),
        ],
    )
    metric_measures = [
        measure_for_metric(
            metric, days_in_period_source=LookMLSqlReferenceType.DIMENSION
        )
        for metric in metrics
    ]
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
        type=MetricUnitOfAnalysisType,
        choices=list(MetricUnitOfAnalysisType),
        default=MetricUnitOfAnalysisType.STATE_CODE,
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
    aggregation_level_type: MetricUnitOfAnalysisType,
    print_view: bool,
    output_directory: Optional[str],
) -> None:
    lookml_view = get_lookml_view_for_metrics(
        population=METRIC_POPULATIONS_BY_TYPE[population_type],
        aggregation_level=METRIC_UNITS_OF_ANALYSIS_BY_TYPE[aggregation_level_type],
        metrics=METRICS_BY_POPULATION_TYPE[population_type],
    )

    if print_view:
        print(lookml_view.build())
    if output_directory:
        lookml_view.write(output_directory, source_script_path=__file__)


if __name__ == "__main__":
    args = parse_arguments()
    main(args.population, args.aggregation_level, args.print_view, args.save_dir)
