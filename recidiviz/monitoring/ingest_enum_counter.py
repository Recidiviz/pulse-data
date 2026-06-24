# Recidiviz - a data platform for criminal justice reform
# Copyright (C) 2026 Recidiviz, Inc.
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
"""Emits an opentelemetry counter metric when an ingest enum field encounters an unmapped
raw text value. The metric flows through Cloud Monitoring to a PagerDuty alert
via the pagerduty_alert_forwarder service.
"""
import logging

from recidiviz.monitoring.instruments import get_monitoring_instrument
from recidiviz.monitoring.keys import AttributeKey, CounterInstrumentKey


def log_unmapped_enum(
    *,
    state_code: str,
    enum_cls: type,
    field_name: str,
    ingest_view_name: str,
    raw_text: str,
) -> None:
    """Records that an enum field had no mapping for a raw text value.

    The raw_text is logged but not included as a metric attribute to avoid
    unbounded cardinality in the number of metrics we produce. We will generate
    one alert per enum *field*.
    """
    logging.warning(
        "Unmapped enum value in %s: field=%s enum=%s raw_text=%r ingest_view=%s",
        state_code,
        field_name,
        enum_cls.__name__,
        raw_text,
        ingest_view_name,
    )

    # raw_text is intentionally excluded from metric attributes to avoid unbounded
    # cardinality. Find the specific value in Cloud Logging via the warning above.
    counter = get_monitoring_instrument(CounterInstrumentKey.INGEST_UNMAPPED_ENUM_VALUE)
    counter.add(
        1,
        attributes={
            AttributeKey.REGION: state_code,
            AttributeKey.ENUM_TYPE: enum_cls.__name__,
            AttributeKey.ENUM_FIELD_NAME: field_name,
            AttributeKey.INGEST_VIEW_NAME: ingest_view_name,
        },
    )


def emit_enum_mapping_heartbeat(
    *,
    state_code: str,
    enum_cls: type,
    field_name: str,
    ingest_view_name: str,
) -> None:
    """Tells Cloud Monitoring "we ran the ingest pipeline for this enum
    field and saw no unmapped values", by writing a 0 to the same cumulative
    counter that `log_unmapped_enum` increments.

    Why this is needed: if a pipeline run doesn't hit any unmapped values
    for a given field, `log_unmapped_enum` never fires, and the counter
    doesn't move, meaning there is no metric data for that field from that
    run. The `unmapped_enum_values_in_ingest_pipeline` alert policy can't
    tell "the pipeline ran and the field was fine" apart from "we just
    haven't heard from any worker yet", so any prior incident on the field
    would stay open even though the field is now fine. Writing a 0 sends a
    present "no unmapped values this run" data point that the policy aligns
    to 0 and uses to resolve.

    Adding 0 to a cumulative counter is a no-op for the cumulative value,
    so this is safe to call for every known enum field at the end of every
    run, even fields that already incremented earlier in the same run. For
    a field that did see an unmapped value this run, the counter stays at
    its incremented value and the heartbeat just emits another data point
    at that same value.

    See the `unmapped_enum_values_in_ingest_pipeline` alert policy in the
    `data-platform-alerting` atmos component for how the alignment,
    aggregation, and missing-data settings consume this signal.
    """
    counter = get_monitoring_instrument(CounterInstrumentKey.INGEST_UNMAPPED_ENUM_VALUE)
    counter.add(
        0,
        attributes={
            AttributeKey.REGION: state_code,
            AttributeKey.ENUM_TYPE: enum_cls.__name__,
            AttributeKey.ENUM_FIELD_NAME: field_name,
            AttributeKey.INGEST_VIEW_NAME: ingest_view_name,
        },
    )
