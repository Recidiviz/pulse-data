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
"""Tests for the `enum_mapping_heartbeats` helpers."""
import unittest
from unittest import mock

import apache_beam as beam

from recidiviz.common.demographics import Ethnicity, Race, Sex
from recidiviz.ingest.direct import direct_ingest_regions
from recidiviz.ingest.direct.ingest_mappings.identity_ingest_view_manifest_compiler_delegate import (
    IdentityIngestViewManifestCompilerDelegate,
)
from recidiviz.ingest.direct.ingest_mappings.ingest_view_manifest_collector import (
    IngestViewManifestCollector,
)
from recidiviz.ingest.direct.ingest_mappings.ingest_view_manifest_compiler import (
    IngestViewManifest,
    IngestViewManifestCompiler,
)
from recidiviz.ingest.direct.types.ingest_pipeline_type import IngestPipelineType
from recidiviz.pipelines.ingest.enum_mapping_heartbeats import (
    _emit_heartbeats_and_flush,
    attach_enum_mapping_heartbeats,
)


def _compile_us_oz_manifest(view_name: str) -> IngestViewManifest:
    region = direct_ingest_regions.get_direct_ingest_region(region_code="us_oz")
    delegate = IdentityIngestViewManifestCompilerDelegate(region=region)
    return IngestViewManifestCompiler(delegate=delegate).compile_manifest(
        ingest_view_name=view_name
    )


def _us_oz_identity_manifest_collector() -> IngestViewManifestCollector:
    region = direct_ingest_regions.get_direct_ingest_region(region_code="us_oz")
    return IngestViewManifestCollector(
        region=region,
        delegate=IdentityIngestViewManifestCompilerDelegate(region=region),
        ingest_pipeline_type=IngestPipelineType.IDENTITY,
    )


class AttachEnumMappingHeartbeatsTest(unittest.TestCase):
    """Tests for `attach_enum_mapping_heartbeats`.

    These run the attached branch through a real Beam pipeline. We patch the
    inner calls (`emit_enum_mapping_heartbeat`, `get_global_meter_provider`)
    rather than `_emit_heartbeats_and_flush` itself: Beam pickles the `beam.Map`
    lambda by value, so patching `_emit_heartbeats_and_flush` directly would
    serialize a copy of the mock and lose our reference to it.
    """

    @mock.patch(
        "recidiviz.pipelines.ingest.enum_mapping_heartbeats.get_global_meter_provider"
    )
    @mock.patch(
        "recidiviz.pipelines.ingest.enum_mapping_heartbeats.emit_enum_mapping_heartbeat"
    )
    def test_non_sandbox_run_emits_heartbeats_for_run_views(
        self,
        mock_emit_heartbeat: mock.MagicMock,
        mock_meter_provider: mock.MagicMock,
    ) -> None:
        collector = _us_oz_identity_manifest_collector()
        with beam.Pipeline() as p:
            merged = p | beam.Create([("US_OZ_IDENTITY_PERSON_ID", "fragment")])
            attach_enum_mapping_heartbeats(
                merged_results=merged,
                is_sandbox_pipeline=False,
                state_code="US_OZ",
                ingest_manifest_collector=collector,
                view_names=["person", "staff"],
            )

        # The branch fired: heartbeats were emitted for the run views (only
        # `person` has enum fields), all tagged with the passed state code, and
        # the meter provider was flushed once.
        self.assertTrue(mock_emit_heartbeat.called)
        self.assertEqual(
            {"US_OZ"},
            {call.kwargs["state_code"] for call in mock_emit_heartbeat.call_args_list},
        )
        self.assertEqual(
            {"person"},
            {
                call.kwargs["ingest_view_name"]
                for call in mock_emit_heartbeat.call_args_list
            },
        )
        mock_meter_provider.return_value.force_flush.assert_called_once()

    @mock.patch(
        "recidiviz.pipelines.ingest.enum_mapping_heartbeats.get_global_meter_provider"
    )
    @mock.patch(
        "recidiviz.pipelines.ingest.enum_mapping_heartbeats.emit_enum_mapping_heartbeat"
    )
    def test_sandbox_run_does_not_emit(
        self,
        mock_emit_heartbeat: mock.MagicMock,
        mock_meter_provider: mock.MagicMock,
    ) -> None:
        collector = _us_oz_identity_manifest_collector()
        with beam.Pipeline() as p:
            merged = p | beam.Create([("US_OZ_IDENTITY_PERSON_ID", "fragment")])
            attach_enum_mapping_heartbeats(
                merged_results=merged,
                is_sandbox_pipeline=True,
                state_code="US_OZ",
                ingest_manifest_collector=collector,
                view_names=["person", "staff"],
            )

        mock_emit_heartbeat.assert_not_called()
        mock_meter_provider.return_value.force_flush.assert_not_called()


class EmitHeartbeatsAndFlushTest(unittest.TestCase):
    """Tests for `_emit_heartbeats_and_flush`."""

    @mock.patch(
        "recidiviz.pipelines.ingest.enum_mapping_heartbeats.get_global_meter_provider"
    )
    @mock.patch(
        "recidiviz.pipelines.ingest.enum_mapping_heartbeats.emit_enum_mapping_heartbeat"
    )
    def test_emits_heartbeat_for_every_enum_field(
        self,
        mock_emit_heartbeat: mock.MagicMock,
        mock_meter_provider: mock.MagicMock,
    ) -> None:
        # The US_OZ `person` manifest maps sex, race, and ethnicity enum fields;
        # the `staff` manifest maps no enum fields.
        manifests = {
            "person": _compile_us_oz_manifest("person"),
            "staff": _compile_us_oz_manifest("staff"),
        }

        _emit_heartbeats_and_flush("US_OZ", manifests)

        emitted = {
            (
                call.kwargs["state_code"],
                call.kwargs["enum_cls"],
                call.kwargs["field_name"],
                call.kwargs["ingest_view_name"],
            )
            for call in mock_emit_heartbeat.call_args_list
        }
        # The walk recurses through every entity nesting level that references an
        # enum, so each enum surfaces under its leaf field, any list field, and
        # the parent `attributes` node (e.g. Sex under both `sex` and
        # `attributes`, Race under `race`, `races`, and `attributes`). Emitting
        # the same (enum, field) more than once is a harmless no-op because the
        # heartbeat only ever adds 0 to the counter.
        self.assertEqual(
            {
                ("US_OZ", Sex, "sex", "person"),
                ("US_OZ", Sex, "attributes", "person"),
                ("US_OZ", Race, "race", "person"),
                ("US_OZ", Race, "races", "person"),
                ("US_OZ", Race, "attributes", "person"),
                ("US_OZ", Ethnicity, "ethnicity", "person"),
                ("US_OZ", Ethnicity, "attributes", "person"),
            },
            emitted,
        )
        mock_meter_provider.return_value.force_flush.assert_called_once()

    @mock.patch(
        "recidiviz.pipelines.ingest.enum_mapping_heartbeats.get_global_meter_provider"
    )
    @mock.patch(
        "recidiviz.pipelines.ingest.enum_mapping_heartbeats.emit_enum_mapping_heartbeat"
    )
    def test_no_enum_fields_still_flushes(
        self,
        mock_emit_heartbeat: mock.MagicMock,
        mock_meter_provider: mock.MagicMock,
    ) -> None:
        _emit_heartbeats_and_flush("US_OZ", {"staff": _compile_us_oz_manifest("staff")})

        mock_emit_heartbeat.assert_not_called()
        mock_meter_provider.return_value.force_flush.assert_called_once()
