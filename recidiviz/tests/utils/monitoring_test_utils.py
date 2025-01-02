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
""" Utilities for testing OpenTelemetry integrations"""
from typing import Any, Optional, Sequence
from unittest.mock import patch

from more_itertools import one
from opentelemetry.sdk.metrics import MeterProvider
from opentelemetry.sdk.metrics.export import (
    DataT,
    InMemoryMetricReader,
    MetricsData,
    ResourceMetrics,
    ScopeMetrics,
)
from opentelemetry.sdk.trace import ReadableSpan, TracerProvider
from opentelemetry.sdk.trace.export import (
    SimpleSpanProcessor,
    SpanExporter,
    SpanExportResult,
)
from opentelemetry.sdk.trace.sampling import TraceIdRatioBased

from recidiviz.monitoring.instruments import reset_instrument_cache
from recidiviz.monitoring.keys import InstrumentEnum
from recidiviz.monitoring.views import build_monitoring_views
from recidiviz.utils.types import assert_type


class SpanExportCapture(SpanExporter):
    """Dummy Span Exporter that captures the last batch of spans"""

    latest_spans: Sequence[ReadableSpan]

    def __init__(self) -> None:
        self.latest_spans = []

    def export(self, spans: Sequence[ReadableSpan]) -> SpanExportResult:
        self.latest_spans = spans
        return SpanExportResult.SUCCESS


class OTLMock:
    """Utility for setting up test meter / tracer providers"""

    _get_tracer_provider_patcher: Any
    _get_meter_provider_patcher: Any
    meter_provider: MeterProvider
    tracer_provider: TracerProvider
    span_export_capture: SpanExportCapture
    metric_reader: InMemoryMetricReader

    def set_up(self) -> None:
        # Reset monitoring instrument cache to clear any Proxy instruments that may have been created outside
        # of working with the OTL mock
        reset_instrument_cache()

        self.span_export_capture = SpanExportCapture()
        self.tracer_provider = TracerProvider(
            sampler=TraceIdRatioBased(rate=100 / 100), shutdown_on_exit=True
        )
        self.tracer_provider.add_span_processor(
            SimpleSpanProcessor(span_exporter=self.span_export_capture)
        )
        self._get_tracer_provider_patcher = patch(
            "recidiviz.monitoring.trace.get_global_tracer_provider",
            return_value=self.tracer_provider,
        )
        self._get_tracer_provider_patcher.start()

        self.metric_reader = InMemoryMetricReader()
        self.meter_provider = MeterProvider(
            metric_readers=[self.metric_reader],
            views=build_monitoring_views(),
        )
        self._get_meter_provider_patcher = patch(
            "recidiviz.monitoring.instruments.get_global_meter_provider",
            return_value=self.meter_provider,
        )
        self._get_meter_provider_patcher.start()

    def tear_down(self) -> None:
        self._get_meter_provider_patcher.stop()
        self._get_tracer_provider_patcher.stop()
        # Clear state for good measure
        reset_instrument_cache()

    def get_latest_span(self) -> ReadableSpan:
        assert len(self.span_export_capture.latest_spans) > 0

        return self.span_export_capture.latest_spans[0]

    def get_metric_data(
        self, metric_name: InstrumentEnum, metric_scope: Optional[str] = "org.recidiviz"
    ) -> DataT:
        recorded_metrics = self.metric_reader.get_metrics_data()
        if not recorded_metrics:
            raise ValueError(f"Could not find any resource metrics for {metric_name}")
        recorded_metrics = assert_type(recorded_metrics, MetricsData)
        resource_metrics: ResourceMetrics = one(recorded_metrics.resource_metrics)
        scope_metrics: ScopeMetrics = one(
            filter(
                lambda scope_metrics: scope_metrics.scope.name == metric_scope,
                resource_metrics.scope_metrics,
            )
        )
        metric = one(
            filter(
                lambda metric: metric.name == metric_name.value, scope_metrics.metrics
            )
        )

        return metric.data
