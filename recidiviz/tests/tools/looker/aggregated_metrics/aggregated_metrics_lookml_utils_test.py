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
"""Tests functionality of aggregated metrics lookml generation function"""

import unittest

from recidiviz.aggregated_metrics.models.aggregated_metric import (
    AssignmentDaysToFirstEventMetric,
    DailyAvgSpanCountMetric,
    DailyAvgTimeSinceSpanStartMetric,
    EventCountMetric,
    SumSpanDaysMetric,
)
from recidiviz.aggregated_metrics.models.aggregated_metric_configurations import (
    AVG_DAILY_POPULATION,
)
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
from recidiviz.observations.event_selector import EventSelector
from recidiviz.observations.event_type import EventType
from recidiviz.observations.span_selector import SpanSelector
from recidiviz.observations.span_type import SpanType
from recidiviz.tools.looker.aggregated_metrics.aggregated_metrics_lookml_utils import (
    custom_description_param_value_builder,
    default_description_param_value_builder,
    generate_lookml_denominator_description_normalized,
    get_metric_description_dimension,
    measure_for_metric,
)

TEST_ASSIGNMENT_METRIC = AssignmentDaysToFirstEventMetric(
    name="test_assignment_metric",
    display_name="Test Assignment Metric",
    description="Description of Test Assignment Metric",
    event_selector=EventSelector(
        event_type=EventType.VIOLATION_RESPONSE,
        event_conditions_dict={},
    ),
    window_length_days=365,
)

TEST_EVENT_METRIC = EventCountMetric(
    name="test_event_metric",
    display_name="Test Event Metric",
    description="Description of test event metric",
    event_selector=EventSelector(
        event_type=EventType.SUPERVISION_TERMINATION_WITH_INCARCERATION_REASON,
        event_conditions_dict={"outflow_to_incarceration": ["false"]},
    ),
)

TEST_POPULATION_METRIC = DailyAvgSpanCountMetric(
    name="test_population_metric",
    display_name="Test Population Metric",
    description="Description of test population metric",
    span_selector=SpanSelector(
        span_type=SpanType.COMPARTMENT_SESSION,
        span_conditions_dict={},
    ),
)

TEST_SPAN_DAYS_METRIC = SumSpanDaysMetric(
    name="test_span_days_metric",
    display_name="Test Span Days Metric",
    description="Description of test span days metric",
    span_selector=SpanSelector(
        span_type=SpanType.JUSTICE_IMPACT_SESSION,
        span_conditions_dict={},
    ),
    weight_col="justice_impact_weight",
)

TEST_VALUE_METRIC = DailyAvgTimeSinceSpanStartMetric(
    name="test_value_metric",
    display_name="Test Value Metric",
    description="Test value metric",
    span_selector=SpanSelector(
        span_type=SpanType.PERSON_DEMOGRAPHICS,
        span_conditions_dict={},
    ),
)


class LookMLUtilsTest(unittest.TestCase):
    """Tests function for lookml generation utils functions"""

    def test_measure_for_metric_custom_denominator_eligible_metric_sql(
        self,
    ) -> None:
        # Check that measure_for_metric returns the correctly formatted LookML measure
        # for a metric type that accepts custom denominators, when allow_custom_denominator = True
        my_measure = measure_for_metric(
            metric=TEST_POPULATION_METRIC,
            days_in_period_source=LookMLSqlReferenceType.DIMENSION,
            param_source_view="my_view",
            allow_custom_denominator=True,
        )
        expected_measure = MeasureLookMLViewField(
            field_name=f"{TEST_POPULATION_METRIC.name}_measure",
            parameters=[
                LookMLFieldParameter.label(TEST_POPULATION_METRIC.display_name),
                LookMLFieldParameter.description(TEST_POPULATION_METRIC.description),
                LookMLFieldParameter.type(LookMLFieldType.NUMBER),
                LookMLFieldParameter.group_label(TEST_POPULATION_METRIC.pretty_name()),
                LookMLFieldParameter.sql(
                    """{% if my_view.measure_type._parameter_value == "normalized" %}
        SUM(SAFE_DIVIDE(${TABLE}.test_population_metric * ${days_in_period}, ${metric_denominator_value})) / SUM(${days_in_period})
        {% else %}
        SUM(${TABLE}.test_population_metric * ${days_in_period}) / SUM(${days_in_period})
        {% endif %}"""
                ),
            ],
        )

        self.assertEqual(my_measure, expected_measure)

    def test_measure_for_metric_no_custom_denominator_eligible_metric_sql(
        self,
    ) -> None:
        # Check that measure_for_metric returns the correctly formatted LookML measure
        # with no custom denominator for a metric that accepts custom denominators,
        # when allow_custom_denominator = False
        my_measure = measure_for_metric(
            metric=TEST_POPULATION_METRIC,
            days_in_period_source=LookMLSqlReferenceType.DIMENSION,
            param_source_view="my_view",
            allow_custom_denominator=False,
        )
        expected_measure = MeasureLookMLViewField(
            field_name=f"{TEST_POPULATION_METRIC.name}_measure",
            parameters=[
                LookMLFieldParameter.label(TEST_POPULATION_METRIC.display_name),
                LookMLFieldParameter.description(TEST_POPULATION_METRIC.description),
                LookMLFieldParameter.type(LookMLFieldType.NUMBER),
                LookMLFieldParameter.group_label(TEST_POPULATION_METRIC.pretty_name()),
                LookMLFieldParameter.sql(
                    """{% if my_view.measure_type._parameter_value == "normalized" %}
        SUM(SAFE_DIVIDE(${TABLE}.test_population_metric * ${days_in_period}, ${TABLE}.avg_daily_population)) / SUM(${days_in_period})
        {% else %}
        SUM(${TABLE}.test_population_metric * ${days_in_period}) / SUM(${days_in_period})
        {% endif %}"""
                ),
            ],
        )

        self.assertEqual(my_measure, expected_measure)

    def test_measure_for_metric_no_custom_denominator_ineligible_metric_sql(
        self,
    ) -> None:
        # Check that measure_for_metric returns the correctly formatted LookML measure
        # with no custom denominator for a metric that does not custom denominators,
        # when allow_custom_denominator = True
        my_measure = measure_for_metric(
            metric=TEST_SPAN_DAYS_METRIC,
            days_in_period_source=LookMLSqlReferenceType.DIMENSION,
            param_source_view="my_view",
            allow_custom_denominator=True,
        )
        expected_measure = MeasureLookMLViewField(
            field_name=f"{TEST_SPAN_DAYS_METRIC.name}_measure",
            parameters=[
                LookMLFieldParameter.label(TEST_SPAN_DAYS_METRIC.display_name),
                LookMLFieldParameter.description(TEST_SPAN_DAYS_METRIC.description),
                LookMLFieldParameter.type(LookMLFieldType.NUMBER),
                LookMLFieldParameter.group_label(TEST_SPAN_DAYS_METRIC.pretty_name()),
                LookMLFieldParameter.sql(
                    """{% if my_view.measure_type._parameter_value == "normalized" %}
        SUM(${TABLE}.test_span_days_metric) / SUM(${TABLE}.avg_daily_population * ${days_in_period})
        {% else %}
        SUM(${TABLE}.test_span_days_metric)
        {% endif %}"""
                ),
            ],
        )

        self.assertEqual(my_measure, expected_measure)

    def test_generate_lookml_denominator_description_normalized(self) -> None:
        self.assertEqual(
            f'"{TEST_ASSIGNMENT_METRIC.description}, '
            'divided by the number of assignments to the population"',
            generate_lookml_denominator_description_normalized(
                TEST_ASSIGNMENT_METRIC, allow_custom_denominator=True
            ),
        )
        self.assertEqual(
            f'CONCAT("{TEST_EVENT_METRIC.description}", ", '
            'divided by the ", ${metric_denominator_description})',
            generate_lookml_denominator_description_normalized(
                TEST_EVENT_METRIC, allow_custom_denominator=True
            ),
        )
        self.assertEqual(
            f'CONCAT("{TEST_EVENT_METRIC.description}", ", '
            'divided by the ", "average daily population")',
            generate_lookml_denominator_description_normalized(
                TEST_EVENT_METRIC, allow_custom_denominator=False
            ),
        )
        self.assertEqual(
            f'"{TEST_SPAN_DAYS_METRIC.description}, '
            'divided by the total number of person-days in the time period"',
            generate_lookml_denominator_description_normalized(
                TEST_SPAN_DAYS_METRIC, allow_custom_denominator=True
            ),
        )
        self.assertEqual(
            f'"{TEST_SPAN_DAYS_METRIC.description}, '
            'divided by the total number of person-days in the time period"',
            generate_lookml_denominator_description_normalized(
                TEST_SPAN_DAYS_METRIC, allow_custom_denominator=True
            ),
        )
        self.assertEqual(
            '""',
            generate_lookml_denominator_description_normalized(
                TEST_VALUE_METRIC, allow_custom_denominator=True
            ),
        )

    def test_default_description_param_value_builder(self) -> None:
        self.assertEqual(
            f'"{AVG_DAILY_POPULATION.description.lower()}"',
            default_description_param_value_builder("avg_daily_population"),
        )

    def test_custom_description_param_value_builder(self) -> None:
        self.assertEqual(
            f"""{{% if supervision_state_aggregated_metrics.measure_type._parameter_value == 'normalized' %}} CONCAT("{AVG_DAILY_POPULATION.description}", ", divided by the ", ${{metric_denominator_description}})
        {{% elsif supervision_state_aggregated_metrics.measure_type._parameter_value == 'value' %}} "{AVG_DAILY_POPULATION.description}"
        {{% endif %}}""",
            custom_description_param_value_builder("avg_daily_population"),
        )

    def test_get_metric_description_dimension_with_custom_builder(self) -> None:
        def my_builder(metric_name: str) -> str:
            return metric_name.upper() + "_customized"

        my_param = ParameterLookMLViewField(
            field_name="my_param",
            parameters=[
                LookMLFieldParameter.type(LookMLFieldType.UNQUOTED),
                LookMLFieldParameter.view_label("my_view_label"),
                LookMLFieldParameter.allowed_value("Option 1", "option_1"),
                LookMLFieldParameter.allowed_value("Option 2", "option_2"),
                LookMLFieldParameter.default_value("option_1"),
            ],
        )
        expected_description_dimension = DimensionLookMLViewField(
            field_name="metric_description",
            parameters=[
                LookMLFieldParameter.type(LookMLFieldType.STRING),
                LookMLFieldParameter.view_label("my_view_label"),
                LookMLFieldParameter.sql(
                    ParameterizedValue(
                        parameter_name="my_view.my_param",
                        parameter_options=["option_1", "option_2"],
                        value_builder=my_builder,
                        indentation_level=3,
                    )
                ),
            ],
        )
        actual_description_dimension = get_metric_description_dimension(
            view_name="my_view",
            metric_filter_parameter=my_param,
            field_name="metric_description",
            custom_description_builder=my_builder,
        )
        self.assertEqual(expected_description_dimension, actual_description_dimension)
