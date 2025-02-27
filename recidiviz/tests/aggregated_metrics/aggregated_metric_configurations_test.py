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
"""Tests validity of aggregated metric configurations"""

import itertools
import unittest

from recidiviz.aggregated_metrics.aggregated_metric_view_collector import (
    METRICS_BY_POPULATION_TYPE,
)
from recidiviz.aggregated_metrics.models.aggregated_metric import (
    AssignmentSpanValueAtStartMetric,
    DailyAvgSpanValueMetric,
    EventMetricConditionsMixin,
    EventValueMetric,
    SpanMetricConditionsMixin,
    SumSpanDaysMetric,
)
from recidiviz.calculator.query.state.views.analyst_data.models.events import (
    EVENTS_BY_TYPE,
)
from recidiviz.calculator.query.state.views.analyst_data.models.spans import (
    SPANS_BY_TYPE,
)


class MetricsByPopulationTypeTest(unittest.TestCase):
    """
    Checks that all configured metrics in METRICS_BY_POPULATION_TYPE reference
    supported attributes of their underlying span or event dependencies.
    """

    # check that span attribute filters are compatible with source spans for all configured span metrics
    def test_compatible_span_attribute_filters(self) -> None:
        span_metrics = [
            m
            for m in itertools.chain.from_iterable(METRICS_BY_POPULATION_TYPE.values())
            if isinstance(m, SpanMetricConditionsMixin)
        ]
        for metric in span_metrics:
            for span_selector in metric.span_selectors:
                for attribute in span_selector.span_conditions_dict:
                    if (
                        attribute
                        not in SPANS_BY_TYPE[span_selector.span_type].attribute_cols
                    ):
                        raise ValueError(
                            f"Span attribute `{attribute}` is not supported by {span_selector.span_type.value} span. "
                            f"Supported attributes: {SPANS_BY_TYPE[span_selector.span_type].attribute_cols}"
                        )

    # check that event attribute filters are compatible with source events for all configured event metrics
    def test_compatible_event_attribute_filters(self) -> None:
        event_metrics = [
            m
            for m in itertools.chain.from_iterable(METRICS_BY_POPULATION_TYPE.values())
            if isinstance(m, EventMetricConditionsMixin)
        ]
        for metric in event_metrics:
            for event_selector in metric.event_selectors:
                for attribute in event_selector.event_conditions_dict:
                    if (
                        attribute
                        not in EVENTS_BY_TYPE[event_selector.event_type].attribute_cols
                    ):
                        raise ValueError(
                            f"Event attribute `{attribute}` is not supported by {event_selector.event_type.value} event. "
                            f"Supported attributes: {EVENTS_BY_TYPE[event_selector.event_type].attribute_cols}"
                        )

    # check that `event_value_numeric` is compatible with event attributes for all configured EventValue metrics
    def test_compatible_event_value_numeric(self) -> None:
        for metric in itertools.chain.from_iterable(
            METRICS_BY_POPULATION_TYPE.values()
        ):
            if isinstance(metric, EventValueMetric):
                for event in metric.event_types:
                    if (
                        metric.event_value_numeric
                        not in EVENTS_BY_TYPE[event].attribute_cols
                    ):
                        raise ValueError(
                            f"Configured event_value_numeric `{metric.event_value_numeric}` is not supported by "
                            f"{event.value} event. Supported attributes: {EVENTS_BY_TYPE[event].attribute_cols}"
                        )

    # check that `span_value_numeric` is compatible with span attributes for all configured DailyAvgSpanValue
    # and AssignmentSpanValueAtStart metrics
    def test_compatible_span_value_numeric(self) -> None:
        for metric in itertools.chain.from_iterable(
            METRICS_BY_POPULATION_TYPE.values()
        ):
            if isinstance(
                metric, (AssignmentSpanValueAtStartMetric, DailyAvgSpanValueMetric)
            ):
                for span in metric.span_types:
                    if (
                        metric.span_value_numeric
                        not in SPANS_BY_TYPE[span].attribute_cols
                    ):
                        raise ValueError(
                            f"Configured span_value_numeric `{metric.span_value_numeric}` is not supported by "
                            f"{span.value} span. Supported attributes: {SPANS_BY_TYPE[span].attribute_cols}"
                        )

    # check that `weight_col` is compatible with span attributes for all configured SumSpanDays metrics
    def test_compatible_weight_col(self) -> None:
        for metric in itertools.chain.from_iterable(
            METRICS_BY_POPULATION_TYPE.values()
        ):
            if isinstance(metric, SumSpanDaysMetric):
                for span in metric.span_types:
                    if metric.weight_col and (
                        metric.weight_col not in SPANS_BY_TYPE[span].attribute_cols
                    ):
                        raise ValueError(
                            f"Configured weight_col `{metric.weight_col}` is not supported by {span.value} span. "
                            f"Supported attributes: {SPANS_BY_TYPE[span].attribute_cols}"
                        )

    def test_consistent_unit_of_observation_type(self) -> None:
        for metric in itertools.chain.from_iterable(
            METRICS_BY_POPULATION_TYPE.values()
        ):
            if (
                isinstance(metric, SpanMetricConditionsMixin)
                and len(
                    set(
                        selector.unit_of_observation_type
                        for selector in metric.span_selectors
                    )
                )
                > 1
            ):
                raise ValueError(
                    f"More than one unit_of_observation_type found for `{metric.name}` metric. "
                    f"All span selectors must be associated with the same unit_of_observation_type."
                )
            if (
                isinstance(metric, EventMetricConditionsMixin)
                and len(
                    set(
                        selector.unit_of_observation_type
                        for selector in metric.event_selectors
                    )
                )
                > 1
            ):
                raise ValueError(
                    f"More than one unit_of_observation_type found for `{metric.name}` metric. "
                    f"All event selectors must be associated with the same unit_of_observation_type."
                )
