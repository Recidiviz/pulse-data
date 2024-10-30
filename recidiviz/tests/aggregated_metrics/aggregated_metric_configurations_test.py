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
    EventCountMetric,
    EventMetricConditionsMixin,
    EventValueMetric,
    SpanMetricConditionsMixin,
    SumSpanDaysMetric,
)
from recidiviz.observations.event_observation_big_query_view_builder import (
    EventObservationBigQueryViewBuilder,
)
from recidiviz.observations.event_type import EventType
from recidiviz.observations.observation_big_query_view_collector import (
    ObservationBigQueryViewCollector,
)
from recidiviz.observations.span_observation_big_query_view_builder import (
    SpanObservationBigQueryViewBuilder,
)
from recidiviz.observations.span_type import SpanType


class MetricsByPopulationTypeTest(unittest.TestCase):
    """
    Checks that all configured metrics in METRICS_BY_POPULATION_TYPE reference
    supported attributes of their underlying span or event dependencies.
    """

    def setUp(self) -> None:
        collector = ObservationBigQueryViewCollector()
        self.span_builders_by_span_type: dict[
            SpanType, SpanObservationBigQueryViewBuilder
        ] = {b.span_type: b for b in collector.collect_span_builders()}
        self.event_builders_by_span_type: dict[
            EventType, EventObservationBigQueryViewBuilder
        ] = {b.event_type: b for b in collector.collect_event_builders()}

    # check that span attribute filters are compatible with source spans for all configured span metrics
    def test_compatible_span_attribute_filters(self) -> None:
        span_metrics = [
            m
            for m in itertools.chain.from_iterable(METRICS_BY_POPULATION_TYPE.values())
            if isinstance(m, SpanMetricConditionsMixin)
        ]
        for metric in span_metrics:
            span_selector = metric.span_selector
            for attribute in span_selector.span_conditions_dict:
                span_type = span_selector.span_type
                supported_attributes = self.span_builders_by_span_type[
                    span_type
                ].attribute_cols

                if attribute not in supported_attributes:
                    raise ValueError(
                        f"Span attribute `{attribute}` is not supported by {span_type.value} span. "
                        f"Supported attributes: {supported_attributes}"
                    )

    # check that event attribute filters are compatible with source events for all configured event metrics
    def test_compatible_event_attribute_filters(self) -> None:
        event_metrics = [
            m
            for m in itertools.chain.from_iterable(METRICS_BY_POPULATION_TYPE.values())
            if isinstance(m, EventMetricConditionsMixin)
        ]
        for metric in event_metrics:
            event_selector = metric.event_selector
            event_type = metric.event_type
            for attribute in event_selector.event_conditions_dict:
                supported_attributes = self.event_builders_by_span_type[
                    event_type
                ].attribute_cols
                if attribute not in supported_attributes:
                    raise ValueError(
                        f"Event attribute `{attribute}` is not supported by {event_type.value} event. "
                        f"Supported attributes: {supported_attributes}"
                    )

    # check that `event_value_numeric` is compatible with event attributes for all configured EventValue metrics
    def test_compatible_event_value_numeric(self) -> None:
        for metric in itertools.chain.from_iterable(
            METRICS_BY_POPULATION_TYPE.values()
        ):
            if not isinstance(metric, EventValueMetric):
                continue
            event = metric.event_type
            supported_attributes = self.event_builders_by_span_type[
                event
            ].attribute_cols
            if metric.event_value_numeric in supported_attributes:
                continue
            raise ValueError(
                f"Configured event_value_numeric `{metric.event_value_numeric}` "
                f"referenced by metric [{metric.name}] is not supported by "
                f"{event.value} event. Supported attributes: {supported_attributes}"
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
                span = metric.span_selector.span_type
                supported_attributes = self.span_builders_by_span_type[
                    span
                ].attribute_cols
                if metric.span_value_numeric not in supported_attributes:
                    raise ValueError(
                        f"Configured span_value_numeric `{metric.span_value_numeric}` is not supported by "
                        f"{span.value} span. Supported attributes: {supported_attributes}"
                    )

    # check that `weight_col` is compatible with span attributes for all configured SumSpanDays metrics
    def test_compatible_weight_col(self) -> None:
        for metric in itertools.chain.from_iterable(
            METRICS_BY_POPULATION_TYPE.values()
        ):
            if isinstance(metric, SumSpanDaysMetric):
                span = metric.span_selector.span_type
                supported_attributes = self.span_builders_by_span_type[
                    span
                ].attribute_cols
                if metric.weight_col and (
                    metric.weight_col not in supported_attributes
                ):
                    raise ValueError(
                        f"Configured weight_col `{metric.weight_col}` is not supported by {span.value} span. "
                        f"Supported attributes: {supported_attributes}"
                    )

    # Check that EventCount distinct attribute columns are all supported Event attributes
    def test_compatible_event_segmentation_columns(self) -> None:
        for metric in itertools.chain.from_iterable(
            METRICS_BY_POPULATION_TYPE.values()
        ):
            if (
                isinstance(metric, EventCountMetric)
                and metric.event_segmentation_columns
            ):
                event = metric.event_type
                for col in metric.event_segmentation_columns:
                    supported_attributes = self.event_builders_by_span_type[
                        event
                    ].attribute_cols
                    if col not in supported_attributes:
                        raise ValueError(
                            f"Configured event_segmentation_columns `{col}` is not supported by "
                            f"{event.value} event. Supported attributes: {supported_attributes}"
                        )
