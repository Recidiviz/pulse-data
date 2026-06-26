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
"""Tests for manifest_compiler_delegate_for_pipeline_type."""

import unittest
from typing import cast

from recidiviz.ingest.direct.ingest_mappings.activity_ingest_view_manifest_compiler_delegate import (
    ActivityIngestViewManifestCompilerDelegate,
)
from recidiviz.ingest.direct.ingest_mappings.identity_ingest_view_manifest_compiler_delegate import (
    IdentityIngestViewManifestCompilerDelegate,
)
from recidiviz.ingest.direct.ingest_mappings.manifest_compiler_delegate_factory import (
    manifest_compiler_delegate_for_pipeline_type,
)
from recidiviz.ingest.direct.types.ingest_pipeline_type import IngestPipelineType
from recidiviz.tests.ingest.direct import fake_regions
from recidiviz.tests.utils.fake_region import fake_region


class TestManifestCompilerDelegateForPipelineType(unittest.TestCase):
    """Tests for `manifest_compiler_delegate_for_pipeline_type`."""

    def setUp(self) -> None:
        self.region = fake_region(
            region_code="us_xx",
            environment="staging",
            region_module=fake_regions,
        )

    def test_activity_returns_activity_delegate(self) -> None:
        delegate = manifest_compiler_delegate_for_pipeline_type(
            region=self.region,
            ingest_pipeline_type=IngestPipelineType.ACTIVITY,
        )
        self.assertIsInstance(delegate, ActivityIngestViewManifestCompilerDelegate)

    def test_identity_returns_identity_delegate(self) -> None:
        delegate = manifest_compiler_delegate_for_pipeline_type(
            region=self.region,
            ingest_pipeline_type=IngestPipelineType.IDENTITY,
        )
        self.assertIsInstance(delegate, IdentityIngestViewManifestCompilerDelegate)

    def test_every_pipeline_type_is_handled(self) -> None:
        for ingest_pipeline_type in IngestPipelineType:
            with self.subTest(ingest_pipeline_type=ingest_pipeline_type):
                manifest_compiler_delegate_for_pipeline_type(
                    region=self.region,
                    ingest_pipeline_type=ingest_pipeline_type,
                )

    def test_unhandled_pipeline_type_raises(self) -> None:
        with self.assertRaisesRegex(
            ValueError, r"^Unexpected ingest_pipeline_type: \[BOGUS\]$"
        ):
            manifest_compiler_delegate_for_pipeline_type(
                region=self.region,
                ingest_pipeline_type=cast(IngestPipelineType, "BOGUS"),
            )
