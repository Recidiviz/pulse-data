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
        for metric in itertools.chain.from_iterable(
            METRICS_BY_POPULATION_TYPE.values()
        ):
            if isinstance(metric, SpanMetricConditionsMixin):
                for span, attribute in itertools.product(
                    metric.span_types, metric.span_attribute_filters
                ):
                    if attribute not in SPANS_BY_TYPE[span].attribute_cols:
                        raise ValueError(
                            f"Span attribute `{attribute}` is not supported by {span.value} span. "
                            f"Supported attributes: {SPANS_BY_TYPE[span].attribute_cols}"
                        )

    # check that event attribute filters are compatible with source events for all configured event metrics
    def test_compatible_event_attribute_filters(self) -> None:
        for metric in itertools.chain.from_iterable(
            METRICS_BY_POPULATION_TYPE.values()
        ):
            if isinstance(metric, EventMetricConditionsMixin):
                for event, attribute in itertools.product(
                    metric.event_types, metric.event_attribute_filters
                ):
                    if attribute not in EVENTS_BY_TYPE[event].attribute_cols:
                        raise ValueError(
                            f"Event attribute `{attribute}` is not supported by {event.value} event. "
                            f"Supported attributes: {EVENTS_BY_TYPE[event].attribute_cols}"
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
