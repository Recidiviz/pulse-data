# Recidiviz - a data platform for criminal justice reform
# Copyright (C) 2021 Recidiviz, Inc.
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
"""Methods for creating OTL Providers. Providers are then entrypoint into the OpenTelemetry APIs"""
import logging
from typing import Optional, cast

from opentelemetry.exporter.cloud_monitoring import CloudMonitoringMetricsExporter
from opentelemetry.exporter.cloud_trace import CloudTraceSpanExporter
from opentelemetry.metrics import get_meter_provider
from opentelemetry.resourcedetector.gcp_resource_detector import (
    GoogleCloudResourceDetector,
)
from opentelemetry.sdk.metrics import MeterProvider
from opentelemetry.sdk.metrics.export import (
    InMemoryMetricReader,
    MetricReader,
    PeriodicExportingMetricReader,
)
from opentelemetry.sdk.resources import Resource, get_aggregated_resources
from opentelemetry.sdk.trace import TracerProvider
from opentelemetry.sdk.trace.export import (
    BatchSpanProcessor,
    ConsoleSpanExporter,
    SpanExporter,
)
from opentelemetry.sdk.trace.sampling import Sampler, TraceIdRatioBased
from opentelemetry.trace import get_tracer_provider

from recidiviz.monitoring.keys import InstrumentEnum
from recidiviz.monitoring.views import build_monitoring_views
from recidiviz.utils.environment import in_development, in_gcp, in_test

CUSTOM_METRIC_NAMESPACE = "custom.googleapis.com/opencensus"


def get_global_tracer_provider() -> TracerProvider:
    """Returns the globally-configured provider object that can be used to generate OpenTelemetry
    Tracer objects (see https://opentelemetry-python.readthedocs.io/en/latest/api/trace.html#opentelemetry.trace.Tracer)
    """
    # A ProxyTracerProvider is returned when a global meter provider is not configured.
    # It maintains the same interface, so cast it
    return cast(TracerProvider, get_tracer_provider())


def get_global_meter_provider() -> MeterProvider:
    """Returns the globally-configured provider object that can be used to generate OpenTelemetry
    Meter objects (see https://opentelemetry-python.readthedocs.io/en/latest/api/metrics.html#module-opentelemetry.metrics)
    """
    # A ProxyMeterProvider is returned when a global meter provider is not configured.
    # It maintains the same interface, so cast it
    return cast(MeterProvider, get_meter_provider())


def detect_resource() -> Resource:
    if in_development():
        return Resource.get_empty()

    return get_aggregated_resources([GoogleCloudResourceDetector(raise_on_error=True)])


def create_monitoring_meter_provider(
    resource: Optional[Resource] = None,
) -> MeterProvider:
    """
    Creates the MeterProvider that coordinates export of our monitoring metrics
    ---
    The OpenTelemetry Metrics API consists of these main components:

        - MeterProvider is the entry point of the API. It provides access to Meters.
        - Meter is the class responsible for creating Instruments.
        - Instrument is responsible for reporting Measurements.

    Here is an example of the object hierarchy inside a process instrumented with the metrics API:

    +-- MeterProvider(default)
        |
        +-- Meter(name='io.opentelemetry.runtime', version='1.0.0')
        |   |
        |   +-- Instrument<Asynchronous Gauge, int>(name='cpython.gc', attributes=['generation'], unit='kB')
        |   |
        |   +-- instruments...
        |
        +-- Meter(name='org.recidiviz')
            |
            +-- Instrument<Counter, int>(name='client.exception', attributes=['type'], unit='1')
            |
            +-- instruments...
    """
    metric_reader: MetricReader
    if in_gcp():
        # In GCP, we export metrics to Cloud Monitoring every 60 seconds
        metric_reader = PeriodicExportingMetricReader(
            CloudMonitoringMetricsExporter(
                prefix=CUSTOM_METRIC_NAMESPACE,
                # Add a unique identifier to exported metrics to avoid metric tuple write ratelimits
                # https://github.com/GoogleCloudPlatform/opentelemetry-operations-python/blob/main/docs/examples/cloud_monitoring/README.rst#troubleshooting
                add_unique_identifier=True,
            ),
            export_interval_millis=60000,
            export_timeout_millis=120000,
        )

        resource = detect_resource()
    else:
        if in_test():
            logging.warning(
                "This tracer provider configuration is not suited for tests; Use OTLMock"
            )

        # In development, we still exercise the OTL code paths but push metrics them to an in-process memory store
        metric_reader = InMemoryMetricReader()

    return MeterProvider(
        metric_readers=[metric_reader],
        resource=resource or Resource.get_empty(),
        views=build_monitoring_views(),
        # Flush metrics when process exits
        shutdown_on_exit=True,
    )


def create_monitoring_tracer_provider(
    sampler: Optional[Sampler] = None,
    resource: Optional[Resource] = None,
) -> TracerProvider:
    """Creates a TracerProvider that traces all endpoints (if an alternate sampler is not provided)
    ---
    The Tracing API consist of these main classes:

        - TracerProvider is the entry point of the API. It provides access to Tracers.
        - Tracer is the class responsible for creating Spans.
        - Span is the API to trace an operation.
    """
    span_exporter: SpanExporter
    if in_gcp():
        span_exporter = CloudTraceSpanExporter()
    else:
        if in_test():
            logging.warning(
                "This meter provider configuration is not suited for tests; Use OTLMock"
            )

        span_exporter = ConsoleSpanExporter()

    tracer_provider = TracerProvider(
        sampler=sampler or TraceIdRatioBased(rate=100 / 100),
        resource=resource or Resource.get_empty(),
        # Flush traces when process exits
        shutdown_on_exit=True,
    )
    tracer_provider.add_span_processor(BatchSpanProcessor(span_exporter))
    return tracer_provider


def monitoring_metric_url(instrument_key: InstrumentEnum) -> str:
    return f"{CUSTOM_METRIC_NAMESPACE}/{instrument_key.value}"
