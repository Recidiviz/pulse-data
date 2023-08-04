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

from recidiviz.aggregated_metrics.models.aggregated_metric_configurations import (
    AVG_DAILY_POPULATION,
    PERSON_DAYS_WEIGHTED_JUSTICE_IMPACT,
)
from recidiviz.looker.lookml_view_field import (
    LookMLFieldParameter,
    MeasureLookMLViewField,
)
from recidiviz.looker.lookml_view_field_parameter import (
    LookMLFieldType,
    LookMLSqlReferenceType,
)
from recidiviz.tools.looker.aggregated_metrics.aggregated_metrics_lookml_utils import (
    measure_for_metric,
)


class LookMLUtilsTest(unittest.TestCase):
    """Tests function for lookml generation utils functions"""

    def test_measure_for_metric_custom_denominator_eligible_metric_sql(
        self,
    ) -> None:
        # Check that measure_for_metric returns the correctly formatted LookML measure
        # for a metric type that accepts custom denominators, when allow_custom_denominator = True
        my_measure = measure_for_metric(
            metric=AVG_DAILY_POPULATION,
            days_in_period_source=LookMLSqlReferenceType.DIMENSION,
            param_source_view="my_view",
            allow_custom_denominator=True,
        )
        expected_measure = MeasureLookMLViewField(
            field_name=f"{AVG_DAILY_POPULATION.name}_measure",
            parameters=[
                LookMLFieldParameter.label(AVG_DAILY_POPULATION.display_name),
                LookMLFieldParameter.description(AVG_DAILY_POPULATION.description),
                LookMLFieldParameter.type(LookMLFieldType.NUMBER),
                LookMLFieldParameter.group_label(AVG_DAILY_POPULATION.pretty_name()),
                LookMLFieldParameter.sql(
                    """{% if my_view.measure_type._parameter_value == "normalized" %}
        SUM(SAFE_DIVIDE(${TABLE}.avg_daily_population * ${days_in_period}, ${metric_denominator_value})) / SUM(${days_in_period})
        {% else %}
        SUM(${TABLE}.avg_daily_population * ${days_in_period}) / SUM(${days_in_period})
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
            metric=AVG_DAILY_POPULATION,
            days_in_period_source=LookMLSqlReferenceType.DIMENSION,
            param_source_view="my_view",
            allow_custom_denominator=False,
        )
        expected_measure = MeasureLookMLViewField(
            field_name=f"{AVG_DAILY_POPULATION.name}_measure",
            parameters=[
                LookMLFieldParameter.label(AVG_DAILY_POPULATION.display_name),
                LookMLFieldParameter.description(AVG_DAILY_POPULATION.description),
                LookMLFieldParameter.type(LookMLFieldType.NUMBER),
                LookMLFieldParameter.group_label(AVG_DAILY_POPULATION.pretty_name()),
                LookMLFieldParameter.sql(
                    """{% if my_view.measure_type._parameter_value == "normalized" %}
        SUM(SAFE_DIVIDE(${TABLE}.avg_daily_population * ${days_in_period}, ${TABLE}.avg_daily_population)) / SUM(${days_in_period})
        {% else %}
        SUM(${TABLE}.avg_daily_population * ${days_in_period}) / SUM(${days_in_period})
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
            metric=PERSON_DAYS_WEIGHTED_JUSTICE_IMPACT,
            days_in_period_source=LookMLSqlReferenceType.DIMENSION,
            param_source_view="my_view",
            allow_custom_denominator=True,
        )
        expected_measure = MeasureLookMLViewField(
            field_name=f"{PERSON_DAYS_WEIGHTED_JUSTICE_IMPACT.name}_measure",
            parameters=[
                LookMLFieldParameter.label(
                    PERSON_DAYS_WEIGHTED_JUSTICE_IMPACT.display_name
                ),
                LookMLFieldParameter.description(
                    PERSON_DAYS_WEIGHTED_JUSTICE_IMPACT.description
                ),
                LookMLFieldParameter.type(LookMLFieldType.NUMBER),
                LookMLFieldParameter.group_label(
                    PERSON_DAYS_WEIGHTED_JUSTICE_IMPACT.pretty_name()
                ),
                LookMLFieldParameter.sql(
                    """{% if my_view.measure_type._parameter_value == "normalized" %}
        SUM(${TABLE}.person_days_weighted_justice_impact) / SUM(${TABLE}.avg_daily_population * ${days_in_period})
        {% else %}
        SUM(${TABLE}.person_days_weighted_justice_impact)
        {% endif %}"""
                ),
            ],
        )

        self.assertEqual(my_measure, expected_measure)
