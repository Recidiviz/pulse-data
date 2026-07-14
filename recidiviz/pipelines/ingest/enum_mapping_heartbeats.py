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
"""Shared helper for emitting enum-mapping heartbeats at the end of ingest view
parsing, used by both the activity and identity ingest pipelines."""
from typing import TypeVar

import apache_beam as beam

from recidiviz.ingest.direct.ingest_mappings.ingest_view_manifest import (
    EntityTreeManifest,
    EnumMappingManifest,
)
from recidiviz.ingest.direct.ingest_mappings.ingest_view_manifest_collector import (
    IngestViewManifestCollector,
)
from recidiviz.ingest.direct.ingest_mappings.ingest_view_manifest_compiler import (
    IngestViewManifest,
)
from recidiviz.monitoring.ingest_enum_counter import emit_enum_mapping_heartbeat
from recidiviz.monitoring.providers import get_global_meter_provider

_T = TypeVar("_T")


def attach_enum_mapping_heartbeats(
    *,
    merged_results: beam.PCollection[_T],
    is_sandbox_pipeline: bool,
    state_code: str,
    ingest_manifest_collector: IngestViewManifestCollector,
    view_names: list[str],
) -> None:
    """Attaches a terminal Beam branch that emits `counter.add(0)` heartbeats for
    every enum field once all ingest view parsing has completed.

    Lets the alert policy see rate=0 for fields with no unmapped values (rather than
    absent data, which would leave existing alerts open).

    Chained after `merged_results` with a `beam.combiners.Count.Globally` so it runs
    only once `merged_results` is completely hydrated. Otherwise a zero-count flush
    could prematurely resolve an active alert.

    No-op for sandbox pipelines, whose heartbeats would pollute the prod alert
    signal.
    """
    if is_sandbox_pipeline:
        return
    manifests_for_views_to_run = {
        view_name: ingest_manifest_collector.ingest_view_to_manifest[view_name]
        for view_name in view_names
    }
    _ = (
        merged_results
        | "Wait for all parsing to complete" >> beam.combiners.Count.Globally()
        | "Emit enum mapping heartbeats"
        >> beam.Map(
            lambda _, sc=state_code, m=manifests_for_views_to_run: _emit_heartbeats_and_flush(
                sc, m
            ),
        )
    )


def _emit_heartbeats_and_flush(
    state_code: str,
    manifests_for_views_to_run: dict[str, IngestViewManifest],
) -> None:
    """Emits `counter.add(0)` heartbeats for all enum fields, then flushes the
    meter provider to ensure metrics are exported before the worker shuts down."""
    for ingest_view_name, manifest in manifests_for_views_to_run.items():
        for entity_node in manifest.output.all_nodes_referenced_with_type(
            EntityTreeManifest
        ):
            for field_name, field_manifest in entity_node.field_manifests.items():
                for enum_manifest in field_manifest.all_nodes_referenced_with_type(
                    EnumMappingManifest
                ):
                    emit_enum_mapping_heartbeat(
                        state_code=state_code,
                        enum_cls=enum_manifest.result_type,
                        field_name=field_name,
                        ingest_view_name=ingest_view_name,
                    )
    get_global_meter_provider().force_flush()
