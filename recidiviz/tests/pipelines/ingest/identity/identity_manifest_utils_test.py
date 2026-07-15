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
"""Tests for get_view_person_type."""
import unittest

import attr

from recidiviz.common.constants.identity import PersonType
from recidiviz.ingest.direct import direct_ingest_regions
from recidiviz.ingest.direct.ingest_mappings.identity_ingest_view_manifest_compiler_delegate import (
    IdentityIngestViewManifestCompilerDelegate,
)
from recidiviz.ingest.direct.ingest_mappings.ingest_view_manifest import (
    EntityTreeManifest,
)
from recidiviz.ingest.direct.ingest_mappings.ingest_view_manifest_compiler import (
    IngestViewManifest,
    IngestViewManifestCompiler,
)
from recidiviz.pipelines.ingest.identity.identity_manifest_utils import (
    get_view_person_type,
)
from recidiviz.utils.types import assert_type


def _compile_us_oz_manifest(view_name: str) -> IngestViewManifest:
    region = direct_ingest_regions.get_direct_ingest_region(region_code="us_oz")
    delegate = IdentityIngestViewManifestCompilerDelegate(region=region)
    return IngestViewManifestCompiler(delegate=delegate).compile_manifest(
        ingest_view_name=view_name
    )


class GetViewPersonTypeTest(unittest.TestCase):
    """Tests for get_view_person_type."""

    def test_person_view_returns_jii(self) -> None:
        self.assertEqual(
            get_view_person_type(_compile_us_oz_manifest("person")), PersonType.JII
        )

    def test_staff_view_returns_staff(self) -> None:
        self.assertEqual(
            get_view_person_type(_compile_us_oz_manifest("staff")), PersonType.STAFF
        )

    def test_view_without_attributes_raises(self) -> None:
        manifest = _compile_us_oz_manifest("person")
        output_without_attributes = attr.evolve(
            manifest.output,
            field_manifests={
                name: field_manifest
                for name, field_manifest in manifest.output.field_manifests.items()
                if name != "attributes"
            },
        )
        with self.assertRaisesRegex(
            ValueError, r"maps no \[attributes\], so its person type cannot"
        ):
            get_view_person_type(
                attr.evolve(manifest, output=output_without_attributes)
            )

    def test_non_literal_person_type_raises(self) -> None:
        manifest = _compile_us_oz_manifest("person")
        attributes = assert_type(
            manifest.output.field_manifests["attributes"], EntityTreeManifest
        )
        # Swap the literal person_type node for a non-literal one (the `name`
        # subtree) to simulate a view that maps person_type dynamically.
        non_literal_node = attributes.field_manifests["name"]
        attributes_with_bad_person_type = attr.evolve(
            attributes,
            field_manifests={
                **attributes.field_manifests,
                "person_type": non_literal_node,
            },
        )
        output = attr.evolve(
            manifest.output,
            field_manifests={
                **manifest.output.field_manifests,
                "attributes": attributes_with_bad_person_type,
            },
        )
        with self.assertRaisesRegex(
            ValueError, r"must author \[person_type\] as a literal enum"
        ):
            get_view_person_type(attr.evolve(manifest, output=output))
