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
"""Function + typing overloads for getting an instrument"""
from typing import overload

from opentelemetry.metrics import Counter, Histogram, ObservableGauge

from recidiviz.monitoring.configs import (
    RECIDIVIZ_METER_NAME,
    MonitoringConfig,
    RecidivizSupportedOTLInstrument,
)
from recidiviz.monitoring.keys import (
    CounterInstrumentKey,
    HistogramInstrumentKey,
    InstrumentEnum,
    ObservableGaugeInstrumentKey,
)
from recidiviz.monitoring.providers import get_global_meter_provider


@overload
def get_monitoring_instrument(key: CounterInstrumentKey) -> Counter:
    ...


@overload
def get_monitoring_instrument(key: HistogramInstrumentKey) -> Histogram:
    ...


@overload
def get_monitoring_instrument(key: ObservableGaugeInstrumentKey) -> ObservableGauge:
    ...


def get_monitoring_instrument(key: InstrumentEnum) -> RecidivizSupportedOTLInstrument:
    """Given a key from an InstrumentEnum subclass, create an instrument on the Recidiviz Meter"""
    monitoring_config = MonitoringConfig.build()
    instrument_config = monitoring_config.get_instrument_config(key)

    return instrument_config.create_instrument(
        meter=get_global_meter_provider().get_meter(RECIDIVIZ_METER_NAME)
    )
