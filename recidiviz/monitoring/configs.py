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
"""Configurations related to our monitoring instrumentation"""
import importlib
import os
from abc import abstractmethod
from functools import cached_property, lru_cache
from inspect import signature
from typing import Dict, List, Literal, Optional, Union

import attr
import cattrs
import yaml
from opentelemetry.metrics import Counter, Histogram, ObservableGauge
from opentelemetry.sdk.metrics import Meter
from opentelemetry.sdk.metrics._internal.instrument import _Gauge as Gauge

from recidiviz.monitoring.keys import (
    CounterInstrumentKey,
    GaugeInstrumentKey,
    HistogramInstrumentKey,
    InstrumentEnum,
    ObservableGaugeInstrumentKey,
    build_instrument_key,
)

RECIDIVIZ_METER_NAME = "org.recidiviz"


RecidivizSupportedOTLInstrument = Union[Counter, Gauge, ObservableGauge, Histogram]
RecidivizSupportedOTLInstrumentType = type[RecidivizSupportedOTLInstrument]

MONITORING_INSTRUMENTS_YAML_PATH = os.path.join(
    os.path.dirname(__file__), "monitoring_instruments.yaml"
)


@attr.s(auto_attribs=True)
class AggregationConfig:
    """Options for selecting an aggregation, see also recidiviz.monitoring.views.aggregation_config
    https://opentelemetry-python.readthedocs.io/en/latest/sdk/metrics.view.html
    """

    kind: Optional[
        Literal[
            "DefaultAggregation",
            "DropAggregation",
            "ExplicitBucketHistogramAggregation",
            "LastValueAggregation",
            "SumAggregation",
        ]
    ] = attr.ib(default=None)
    options: Optional[Dict] = attr.ib(factory=dict)


@attr.s(auto_attribs=True)
class ViewConfig:
    """Views are used to customize the metric data prior to being exported

    We currently use them to filter out superfluous attributes that are not specified in `attribute_keys` or to
    customize the aggregation used (i.e. to change buckets of a histogram).

    The name can match an instrument key's name to modify its output, or can be written to a separate metric
    """

    attributes: set[str] = attr.ib(factory=set)
    description: Optional[str] = attr.ib(default=None)
    aggregation: Optional[AggregationConfig] = attr.ib(default=None)
    name: Optional[str] = attr.ib(default=None)


@attr.define
class InstrumentConfig:
    instrument_key: InstrumentEnum
    description: Optional[str] = attr.ib(default=None)
    unit: Optional[str] = attr.ib(default="1")
    views: List[ViewConfig] = attr.ib(factory=dict)

    @property
    def common_kwargs(self) -> dict[str, str]:
        return {
            "name": self.instrument_key.value,
            "description": self.description or "",
            "unit": self.unit or "",
        }

    @abstractmethod
    def create_instrument(self, meter: Meter) -> RecidivizSupportedOTLInstrument:
        ...

    @staticmethod
    def get_config_constructor_for_instrument_name(
        instrument_name: str,
    ) -> type["InstrumentConfig"]:
        key = build_instrument_key(instrument_name)

        return INSTRUMENT_KEY_TO_CONFIG[key.__class__]


@attr.s(auto_attribs=True, kw_only=True)
class CounterInstrumentConfig(InstrumentConfig):
    """
    A Counter is a synchronous Instrument which supports non-negative increments.

    An example of use case is to count when certain events occur

    https://opentelemetry.io/docs/specs/otel/metrics/data-model/#sums
    """

    instrument_key: CounterInstrumentKey = attr.ib()

    def create_instrument(self, meter: Meter) -> Counter:
        return meter.create_counter(**self.common_kwargs)


@attr.s(auto_attribs=True, kw_only=True)
class ObservableGaugeInstrumentConfig(InstrumentConfig):
    """
        A Gauge in OTL represents a sampled value at a given time. A Gauge stream consists of:
        A set of data points, each containing:
            - An independent set of Attribute name-value pairs.
            - A sampled value (e.g. current CPU temperature)
            - A timestamp when the value was sampled (time_unix_nano)

        In this case, "Observable" refers to the fact that we are not pushing data to OTL, instead, we register
        callbacks that are periodically called to yield our observations.

    An example of a current use is exporting metric file creation times in `export_timeliness.py`

    https://opentelemetry.io/docs/specs/otel/metrics/data-model/#gauge
    """

    instrument_key: ObservableGaugeInstrumentKey = attr.ib()
    callbacks: List[str] = attr.ib(factory=list)

    def create_instrument(self, meter: Meter) -> ObservableGauge:
        callbacks = []
        for callback in self.callbacks:
            module_name, method_name = callback.rsplit(".", maxsplit=1)
            callback_func = getattr(importlib.import_module(module_name), method_name)
            callback_signature = signature(callback_func)
            if (
                len(callback_signature.parameters) != 1
                or "callback_options" not in callback_signature.parameters
            ):
                raise ValueError(
                    "Cannot create callback with incorrect signature; must accept one callback_options arg"
                )
            callbacks.append(callback_func)

        return meter.create_observable_gauge(
            **self.common_kwargs,
            callbacks=callbacks,
        )


@attr.s(auto_attribs=True, kw_only=True)
class GaugeInstrumentConfig(InstrumentConfig):
    """
    A Gauge represents a sampled value at a given time, similar to ObservableGauge.
    However, unlike ObservableGauge, Gauge is synchronous and does not require callbacks.
    Instead, observations are created directly by calling the gauge in your code.

    An example use case is reporting metrics that are calculated on-demand rather than
    being observed periodically.

    https://opentelemetry.io/docs/specs/otel/metrics/data-model/#gauge
    """

    instrument_key: GaugeInstrumentKey = attr.ib()

    def create_instrument(self, meter: Meter) -> Gauge:
        # mypy doesn't recognize meter.create_gauge() returns _Gauge which we alias as Gauge
        return meter.create_gauge(**self.common_kwargs)  # type: ignore[return-value]


@attr.s(auto_attribs=True, kw_only=True)
class HistogramInstrumentConfig(InstrumentConfig):
    """
    Histogram instruments' data points convey a population of recorded measurements in a compressed format.
    A histogram bundles a set of events into divided populations with an overall event count and aggregate sum for all
    events.

    A common example would be a set of request latencies, bundled by buckets of 100ms, 200ms, 500ms, 1000ms
    https://opentelemetry.io/docs/specs/otel/metrics/data-model/#histogram
    """

    instrument_key: HistogramInstrumentKey = attr.ib()

    def create_instrument(self, meter: Meter) -> Histogram:
        args = self.common_kwargs
        return meter.create_histogram(
            name=args["name"], description=args["description"], unit=args["unit"]
        )


INSTRUMENT_KEY_TO_CONFIG = {
    CounterInstrumentKey: CounterInstrumentConfig,
    GaugeInstrumentKey: GaugeInstrumentConfig,
    HistogramInstrumentKey: HistogramInstrumentConfig,
    ObservableGaugeInstrumentKey: ObservableGaugeInstrumentConfig,
}


@attr.s(auto_attribs=True)
class MonitoringConfig:
    """Stores all monitoring instrument configuration for instruments belonging to a particular naming scope."""

    scope: str = attr.ib(default=RECIDIVIZ_METER_NAME)
    instruments: list[InstrumentConfig] = attr.ib(factory=list)

    def get_instrument_config(self, key: InstrumentEnum) -> InstrumentConfig:
        return self.instrument_dict[key]

    @cached_property
    def instrument_dict(self) -> Dict[InstrumentEnum, InstrumentConfig]:
        return {
            instrument_config.instrument_key: instrument_config
            for instrument_config in self.instruments
        }

    @classmethod
    @lru_cache
    def build(cls) -> "MonitoringConfig":
        with open(
            MONITORING_INSTRUMENTS_YAML_PATH, mode="r", encoding="utf-8"
        ) as yaml_file:
            monitoring_yaml = yaml.safe_load(yaml_file)

            instruments: list[InstrumentConfig] = []

            strict_converter = cattrs.Converter(forbid_extra_keys=True)

            for instrument in monitoring_yaml["instruments"]:
                instrument_config_cls = (
                    InstrumentConfig.get_config_constructor_for_instrument_name(
                        instrument["instrument_key"]
                    )
                )

                instruments.append(
                    strict_converter.structure(instrument, instrument_config_cls)
                )

            return cls(
                scope=monitoring_yaml["scope"],
                instruments=instruments,
            )
