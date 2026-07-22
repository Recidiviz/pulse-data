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
"""Tests for expected_output_helpers."""
import unittest
from types import ModuleType

from recidiviz.common.constants.tenants import Tenant
from recidiviz.pipelines.ingest.identity.expected_output_helpers import (
    get_valid_external_id_types,
)
from recidiviz.tests.ingest.direct import fake_regions
from recidiviz.tests.pipelines.ingest.identity.identity_ingest_region_test_mixin import (
    IdentityIngestRegionTestMixin,
)


class TestGetValidExternalIdTypes(IdentityIngestRegionTestMixin, unittest.TestCase):
    """Tests for get_valid_external_id_types, run against the fake US_DD
    region's identity manifests."""

    @classmethod
    def tenant(cls) -> Tenant:
        return Tenant.US_DD

    @classmethod
    def region_module_override(cls) -> ModuleType | None:
        return fake_regions

    def test_all_views(self) -> None:
        collector = self.ingest_view_manifest_collector()

        result = get_valid_external_id_types(
            ingest_manifest_collector=collector,
            ingest_view_names=sorted(collector.ingest_view_to_manifest),
        )

        self.assertEqual(
            result,
            frozenset(
                {
                    "US_DD_IDENTITY_PERSON_ID",
                    "US_DD_IDENTITY_STAFF_ID",
                    "US_DD_IDENTITY_BADGE_ID",
                }
            ),
        )

    def test_subset_of_views(self) -> None:
        result = get_valid_external_id_types(
            ingest_manifest_collector=self.ingest_view_manifest_collector(),
            ingest_view_names=["person"],
        )

        self.assertEqual(result, frozenset({"US_DD_IDENTITY_PERSON_ID"}))

    def test_no_views(self) -> None:
        result = get_valid_external_id_types(
            ingest_manifest_collector=self.ingest_view_manifest_collector(),
            ingest_view_names=[],
        )

        self.assertEqual(result, frozenset())
