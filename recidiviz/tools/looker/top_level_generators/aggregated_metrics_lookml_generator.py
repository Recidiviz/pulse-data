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
"""A script for generating LookML view files containing measures for
all configured metrics associated with the incarceration, supervision, and justice-involved
populations.

To generate lookml view files for incarceration, supervision, and justice-involved
populations, run:
    python -m recidiviz.tools.looker.top_level_generators.aggregated_metrics_lookml_generator \
       [--looker-repo-root [DIR]]
"""
from typing import List, Optional, Tuple

from recidiviz.aggregated_metrics.configuration.collections.standard import (
    METRICS_BY_POPULATION_TYPE,
)
from recidiviz.aggregated_metrics.models.aggregated_metric import AggregatedMetric
from recidiviz.aggregated_metrics.models.aggregated_metric_configurations import (
    AVG_DAILY_POPULATION,
    AVG_DAILY_POPULATION_TASK_ELIGIBLE_METRICS_INCARCERATION,
    AVG_DAILY_POPULATION_TASK_ELIGIBLE_METRICS_SUPERVISION,
    DRUG_SCREENS,
    INCARCERATION_STARTS,
    INCARCERATION_STARTS_AND_INFERRED,
    LSIR_ASSESSMENTS,
    PAROLE_BOARD_HEARINGS,
    SUPERVISION_STARTS,
    VIOLATION_RESPONSES,
    VIOLATIONS,
)
from recidiviz.aggregated_metrics.models.metric_population_type import (
    MetricPopulationType,
)
from recidiviz.aggregated_metrics.models.metric_unit_of_analysis_type import (
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
    custom_description_param_value_builder,
    get_metric_description_dimension,
    get_metric_explore_parameter,
    get_metric_value_dimension,
    get_metric_value_measure,
    measure_for_metric,
)
from recidiviz.tools.looker.script_helpers import (
    get_generated_views_path,
    parse_and_validate_output_dir_arg,
)
from recidiviz.tools.looker.top_level_generators.base_lookml_generator import (
    LookMLGenerator,
)

_ALLOWED_METRIC_DENOMINATORS: List[AggregatedMetric] = [
    AVG_DAILY_POPULATION,
    *AVG_DAILY_POPULATION_TASK_ELIGIBLE_METRICS_INCARCERATION,
    *AVG_DAILY_POPULATION_TASK_ELIGIBLE_METRICS_SUPERVISION,
    DRUG_SCREENS,
    LSIR_ASSESSMENTS,
    INCARCERATION_STARTS_AND_INFERRED,
    INCARCERATION_STARTS,
    PAROLE_BOARD_HEARINGS,
    SUPERVISION_STARTS,
    VIOLATION_RESPONSES,
    VIOLATIONS,
]


def get_lookml_views_for_metrics(
    population_type: MetricPopulationType,
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
            allow_custom_denominator=True,
        )
        for metric in metrics
    ]
    metric_filter_parameter = get_metric_explore_parameter(
        metrics,
        field_name="metric_filter",
        default_metric=AVG_DAILY_POPULATION,
    ).extend(
        additional_parameters=[
            LookMLFieldParameter.label(
                f"[{population_type.population_name_title}] Metric Filter"
            ),
            LookMLFieldParameter.description(
                "Used to select one metric for a Look. Works across all levels of "
                f"observation for the {population_type.value} population."
            ),
            LookMLFieldParameter.group_label("Metric Filter"),
        ]
    )
    metric_denominator_filter_parameter = get_metric_explore_parameter(
        [m for m in metrics if m in _ALLOWED_METRIC_DENOMINATORS],
        field_name="metric_denominator_filter",
        default_metric=AVG_DAILY_POPULATION,
    ).extend(
        additional_parameters=[
            LookMLFieldParameter.label(
                f"[{population_type.population_name_title}] Metric Denominator"
            ),
            LookMLFieldParameter.description(
                "Used to select a denominator to normalize the metric if the `normalized` "
                "measure type is selected. Default is average daily population."
            ),
            LookMLFieldParameter.group_label("Metric Denominator"),
        ]
    )
    metric_value_measure = get_metric_value_measure(
        f"{population_type.population_name_short}_{parent_name}_aggregated_metrics",
        metric_filter_parameter,
    )
    metric_denominator_value_dimension = get_metric_value_dimension(
        f"{population_type.population_name_short}_{parent_name}_aggregated_metrics",
        metric_denominator_filter_parameter,
        "metric_denominator_value",
    ).extend(additional_parameters=[LookMLFieldParameter.hidden(is_hidden=True)])
    metric_denominator_description_dimension = get_metric_description_dimension(
        f"{population_type.population_name_short}_{parent_name}_aggregated_metrics",
        metric_denominator_filter_parameter,
        "metric_denominator_description",
    ).extend(
        additional_parameters=[
            LookMLFieldParameter.hidden(is_hidden=True),
            LookMLFieldParameter.description(
                f"Outputs the description associated with the metric chosen using "
                f"`{metric_denominator_filter_parameter.field_name}`"
            ),
        ]
    )

    dynamic_explore_description = get_metric_description_dimension(
        f"{population_type.population_name_short}_{parent_name}_aggregated_metrics",
        metric_filter_parameter,
        "dynamic_description",
        custom_description_builder=custom_description_param_value_builder,
    ).extend(
        additional_parameters=[
            LookMLFieldParameter.description(
                "Outputs the description associated with the selected metric, metric denominator, and measure type"
            ),
        ]
    )
    aggregated_metrics_view = LookMLView(
        view_name=f"{population_type.population_name_short}_aggregated_metrics_template",
        fields=[
            *index_dimensions_hidden,
            *metric_measures,
            metric_value_measure,
            metric_denominator_value_dimension,
            metric_denominator_description_dimension,
            dynamic_explore_description,
        ],
    )
    included_fields: List[LookMLViewField] = [
        metric_filter_parameter,
        metric_denominator_filter_parameter,
    ]
    # Only include the measure_type parameter for the supervision population
    if population_type is MetricPopulationType.SUPERVISION:
        included_fields = [measure_type_parameter] + included_fields
    aggregated_metrics_menu_view = LookMLView(
        view_name=f"{population_type.population_name_short}_aggregated_metrics_menu",
        fields=included_fields,
    )
    return aggregated_metrics_view, aggregated_metrics_menu_view


class AggregatedMetricsLookMLGenerator(LookMLGenerator):
    """Generates LookML views for aggregated metrics."""

    @staticmethod
    def generate_lookml(output_dir: str) -> None:
        output_subdir = get_generated_views_path(
            output_dir=output_dir, module_name="aggregated_metrics"
        )
        for population in [
            MetricPopulationType.INCARCERATION,
            MetricPopulationType.SUPERVISION,
            MetricPopulationType.JUSTICE_INVOLVED,
        ]:
            metrics_view, menu_view = get_lookml_views_for_metrics(
                population_type=population,
                metrics=METRICS_BY_POPULATION_TYPE[population],
            )

            metrics_view.write(output_subdir, source_script_path=__file__)
            menu_view.write(output_subdir, source_script_path=__file__)


if __name__ == "__main__":
    AggregatedMetricsLookMLGenerator.generate_lookml(
        output_dir=parse_and_validate_output_dir_arg()
    )
