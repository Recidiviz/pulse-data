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
"""Emits an opentelemetry gauge metric describing whether an ingest enum field
currently has an unmapped raw text value (1) or is clean (0). The metric flows
through Cloud Monitoring to a PagerDuty alert via the pagerduty_alert_forwarder
service.
"""
import logging

from recidiviz.monitoring.instruments import get_monitoring_instrument
from recidiviz.monitoring.keys import AttributeKey, GaugeInstrumentKey


def log_unmapped_enum(
    *,
    state_code: str,
    enum_cls: type,
    field_name: str,
    ingest_view_name: str,
    raw_text: str,
) -> None:
    """Records that an enum field had no mapping for a raw text value by setting
    the gauge for that field to 1.

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
    gauge = get_monitoring_instrument(GaugeInstrumentKey.INGEST_UNMAPPED_ENUM_VALUE)
    gauge.set(
        amount=1,
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
    """Tells Cloud Monitoring "we ran the ingest pipeline for this enum field and
    saw no unmapped values", by setting the field's gauge to 0.

    Why this is needed: if a pipeline run doesn't hit any unmapped values for a
    given field, `log_unmapped_enum` never fires, so that field emits no data
    point for the run. The `unmapped_enum_values_in_ingest_pipeline` alert policy
    can't tell "the pipeline ran and the field was fine" apart from "we just
    haven't heard from any worker yet", so any prior incident on the field would
    stay open even though the field is now fine. Setting a 0 sends a present "no
    unmapped values this run" data point that the policy resolves on.

    This is safe to call for every known enum field at the end of every run. The
    heartbeat runs as a single `beam.Map` after a `Count.Globally()` barrier, so
    it can share a worker (and thus a series) with a parsing `set(1)`, which
    `set(0)` would then read back as 0. What prevents masking is timing, not
    per-worker series separation: the 60s periodic export ships the parsing `1`
    during the multi-minute barrier wait, before the heartbeat `set(0)`, so
    `ALIGN_MAX` over the window still sees it. Masking would require a full run to
    complete within one 60s export interval, which our ingest runs do not.

    See the `unmapped_enum_values_in_ingest_pipeline` alert policy in the
    `data-platform-alerting` atmos component for how the alignment, aggregation,
    and missing-data settings consume this signal.
    """
    gauge = get_monitoring_instrument(GaugeInstrumentKey.INGEST_UNMAPPED_ENUM_VALUE)
    gauge.set(
        amount=0,
        attributes={
            AttributeKey.REGION: state_code,
            AttributeKey.ENUM_TYPE: enum_cls.__name__,
            AttributeKey.ENUM_FIELD_NAME: field_name,
            AttributeKey.INGEST_VIEW_NAME: ingest_view_name,
        },
    )
