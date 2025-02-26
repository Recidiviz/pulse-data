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
    python -m recidiviz.tools.looker.aggregated_metrics.aggregated_metrics_lookml_generator \
       --population [POPULATION] --save_views_to_dir [PATH]
"""
import argparse
from typing import List, Optional, Tuple

from recidiviz.aggregated_metrics.aggregated_metric_view_collector import (
    METRICS_BY_POPULATION_TYPE,
)
from recidiviz.aggregated_metrics.models.aggregated_metric import AggregatedMetric
from recidiviz.aggregated_metrics.models.aggregated_metric_configurations import (
    AVG_DAILY_POPULATION,
)
from recidiviz.calculator.query.state.views.analyst_data.models.metric_population_type import (
    METRIC_POPULATIONS_BY_TYPE,
    MetricPopulation,
    MetricPopulationType,
)
from recidiviz.calculator.query.state.views.analyst_data.models.metric_unit_of_analysis_type import (
    MetricUnitOfAnalysisType,
)
from recidiviz.looker.lookml_view import LookMLView
from recidiviz.looker.lookml_view_field import (
    DimensionLookMLViewField,
    LookMLFieldParameter,
    LookMLViewField,
    MeasureLookMLViewField,
    ParameterLookMLViewField,
)
from recidiviz.looker.lookml_view_field_parameter import (
    LookMLFieldType,
    LookMLSqlReferenceType,
)
from recidiviz.tools.looker.aggregated_metrics.aggregated_metrics_lookml_utils import (
    get_metric_filter_parameter,
    get_metric_value_measure,
    measure_for_metric,
)


def get_lookml_views_for_metrics(
    population: MetricPopulation,
    metrics: List[AggregatedMetric],
    parent_unit_of_analysis: Optional[MetricUnitOfAnalysisType] = None,
) -> Tuple[LookMLView, LookMLView]:
    """Generates extendable LookML views for the specified population and metrics.
    Optional param `parent_unit_of_analysis` to specify the unit of analysis that forms the root of the explore
    and any relevant shared filters and parameters."""

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
    index_dimensions = [
        DimensionLookMLViewField.for_column("state_code"),
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

    index_dimensions_hidden = [
        dimension.extend(
            additional_parameters=[LookMLFieldParameter.hidden(is_hidden=True)]
        )
        for dimension in index_dimensions
    ]

    measure_type_parameter = ParameterLookMLViewField(
        field_name="measure_type",
        parameters=[
            LookMLFieldParameter.type(LookMLFieldType.UNQUOTED),
            LookMLFieldParameter.description(
                "Used to select whether metric should be presented as a raw value or a normalized rate"
            ),
            LookMLFieldParameter.view_label("Metric Menu"),
            LookMLFieldParameter.allowed_value("Normalized", "normalized"),
            LookMLFieldParameter.allowed_value("Value", "value"),
            LookMLFieldParameter.default_value("value"),
        ],
    )
    # If no parent unit of analysis is specified, default to state.
    parent_name = (
        parent_unit_of_analysis.value.lower()
        if parent_unit_of_analysis
        else MetricUnitOfAnalysisType.STATE_CODE.value.lower()
    )
    metric_measures = [
        measure_for_metric(
            metric,
            days_in_period_source=LookMLSqlReferenceType.DIMENSION,
            # for now, always use supervision_state_aggregated_metrics.measure_type
            param_source_view="supervision_state_aggregated_metrics",
        )
        for metric in metrics
    ]
    metric_filter_parameter = get_metric_filter_parameter(
        metrics,
        population,
        default_metric=AVG_DAILY_POPULATION,
    )
    metric_value_measure = get_metric_value_measure(
        f"{population.population_name_short}_{parent_name}_aggregated_metrics",
        metric_filter_parameter,
    )
    aggregated_metrics_view = LookMLView(
        view_name=f"{population.population_name_short}_aggregated_metrics_template",
        fields=[
            *index_dimensions_hidden,
            *metric_measures,
            metric_value_measure,
        ],
    )
    included_fields: List[LookMLViewField] = [metric_filter_parameter]
    # Only include the measure_type parameter for the supervision population
    if population.population_type is MetricPopulationType.SUPERVISION:
        included_fields = [measure_type_parameter] + included_fields
    aggregated_metrics_menu_view = LookMLView(
        view_name=f"{population.population_name_short}_aggregated_metrics_menu",
        fields=included_fields,
    )
    return aggregated_metrics_view, aggregated_metrics_menu_view


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
        "--save_views_to_dir",
        dest="save_dir",
        help="Specifies name of directory where to save view file",
        type=str,
        required=True,
    )

    return parser.parse_args()


def main(
    population_type: MetricPopulationType,
    save_dir: str,
) -> None:
    metrics_view, menu_view = get_lookml_views_for_metrics(
        population=METRIC_POPULATIONS_BY_TYPE[population_type],
        metrics=METRICS_BY_POPULATION_TYPE[population_type],
    )

    metrics_view.write(save_dir, source_script_path=__file__)
    menu_view.write(save_dir, source_script_path=__file__)


if __name__ == "__main__":
    args = parse_arguments()
    main(args.population, args.save_dir)
