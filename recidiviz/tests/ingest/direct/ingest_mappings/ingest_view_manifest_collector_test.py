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
"""Tests for ingest_view_manifest_collector.py."""
import unittest

from recidiviz.ingest.direct.ingest_mappings.ingest_view_manifest_collector import (
    IngestViewManifestCollector,
)
from recidiviz.ingest.direct.ingest_mappings.ingest_view_manifest_compiler_delegate import (
    IngestViewManifestCompilerDelegateImpl,
)
from recidiviz.ingest.direct.types.direct_ingest_instance import DirectIngestInstance
from recidiviz.persistence.database.schema_type import SchemaType
from recidiviz.tests.ingest.direct import fake_regions
from recidiviz.tests.utils.fake_region import fake_region


class IngestViewManifestCollectorTest(unittest.TestCase):
    """Tests for IngestViewManifestCollector."""

    def setUp(self) -> None:
        us_xx_region = fake_region(
            region_code="us_xx",
            environment="staging",
            region_module=fake_regions,
        )
        self.us_xx_ingest_view_manifest_collector = IngestViewManifestCollector(
            us_xx_region,
            IngestViewManifestCompilerDelegateImpl(
                region=us_xx_region, schema_type=SchemaType.STATE
            ),
        )

        us_yy_region = fake_region(
            region_code="us_yy",
            environment="staging",
            region_module=fake_regions,
        )
        self.us_yy_ingest_view_manifest_collector = IngestViewManifestCollector(
            us_yy_region,
            IngestViewManifestCompilerDelegateImpl(
                region=us_yy_region, schema_type=SchemaType.STATE
            ),
        )

    def test_ingest_view_to_manifest(self) -> None:
        result = self.us_xx_ingest_view_manifest_collector.ingest_view_to_manifest
        self.assertListEqual(
            [
                "basic",
                "tagBasicData",
                "tagMoreBasicData",
            ],
            list(sorted(result.keys())),
        )

    def test_launchable_ingest_views(self) -> None:
        result = self.us_xx_ingest_view_manifest_collector.launchable_ingest_views(
            ingest_instance=DirectIngestInstance.PRIMARY
        )
        self.assertListEqual(
            ["basic", "tagBasicData", "tagMoreBasicData"],
            list(sorted(result)),
        )

        result = self.us_yy_ingest_view_manifest_collector.launchable_ingest_views(
            ingest_instance=DirectIngestInstance.PRIMARY
        )
        self.assertListEqual(
            [],
            list(sorted(result)),
        )

        result = self.us_yy_ingest_view_manifest_collector.launchable_ingest_views(
            ingest_instance=DirectIngestInstance.SECONDARY
        )
        self.assertListEqual(
            # In US_YY this view is gated to only run in SECONDARY
            ["basic"],
            list(sorted(result)),
        )

    def test_parse_ingest_view_name(self) -> None:
        self.assertEqual(
            "some_view",
            # pylint: disable=protected-access
            self.us_xx_ingest_view_manifest_collector._parse_ingest_view_name(
                "my/test/path/us_xx_some_view.yaml"
            ),
        )
        with self.assertRaisesRegex(
            ValueError,
            r"Manifest path does not match expected format.*",
        ):
            # pylint: disable=protected-access
            self.us_xx_ingest_view_manifest_collector._parse_ingest_view_name(
                "my/test/path/bad_view.yaml"
            )

        with self.assertRaisesRegex(
            ValueError,
            r"Manifest path does not match expected format.*",
        ):
            # pylint: disable=protected-access
            self.us_xx_ingest_view_manifest_collector._parse_ingest_view_name(
                "my/test/path/us_xx_bad_view"
            )
