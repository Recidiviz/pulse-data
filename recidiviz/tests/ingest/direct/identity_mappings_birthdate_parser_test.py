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
"""Tests that every identity mapping routes its birthdate through the shared
reasonable_birthdate_or_null parser.

A bare birthdate mapping (birthdate: SOME_COLUMN) passes an out-of-bounds source
value straight to the IdentityAttributes validator, which crashes entity
construction. Routing through reasonable_birthdate_or_null omits the junk value
instead, so the rest of the fragment survives. This test enforces that
convention across every region's identity mappings rather than leaving it to a
reviewer to catch.
"""
import os
from unittest import TestCase

import yaml

from recidiviz.ingest.direct.direct_ingest_regions import get_direct_ingest_region
from recidiviz.ingest.direct.ingest_mappings.ingest_view_manifest_compiler_delegate import (
    IngestViewManifestCompilerDelegate,
    ingest_view_manifest_dir,
    yaml_mappings_filepath,
)
from recidiviz.ingest.direct.regions.direct_ingest_region_utils import (
    get_existing_direct_ingest_states,
)
from recidiviz.ingest.direct.types.ingest_pipeline_type import IngestPipelineType
from recidiviz.pipelines.ingest.identity import date_custom_parsers

# The $function reference every identity birthdate mapping must use, built from
# the real module and parser so a rename of either updates it. The module name
# comes from the module object (not the function's __module__) because the
# function is defined in reasonable_date_validators and re-exported here, while
# mappings must reference it through this module.
EXPECTED_BIRTHDATE_FUNCTION_REFERENCE = (
    f"{date_custom_parsers.__name__.rsplit('.', 1)[-1]}"
    f".{date_custom_parsers.reasonable_birthdate_or_null.__name__}"
)


class IdentityMappingsBirthdateParserTest(TestCase):
    """Tests that every identity mapping's birthdate routes through
    reasonable_birthdate_or_null."""

    def test_every_birthdate_mapping_uses_the_parser(self) -> None:
        for state_code in get_existing_direct_ingest_states():
            region = get_direct_ingest_region(region_code=state_code.value.lower())
            mappings_dir = ingest_view_manifest_dir(region, IngestPipelineType.IDENTITY)
            view_names = IngestViewManifestCompilerDelegate.get_ingest_view_names_from_mappings_dir(
                mappings_dir, region.region_code
            )
            for view_name in view_names:
                mapping_path = yaml_mappings_filepath(
                    region=region,
                    ingest_view_name=view_name,
                    ingest_pipeline_type=IngestPipelineType.IDENTITY,
                )
                with self.subTest(mapping=os.path.basename(mapping_path)):
                    with open(mapping_path, encoding="utf-8") as mapping_file:
                        manifest = yaml.safe_load(mapping_file)

                    attributes = manifest["output"]["IdentityFragment"].get(
                        "attributes"
                    )
                    if not attributes:
                        # External-id-only fragments (e.g. the badge association
                        # views) carry no attributes, so there is no birthdate.
                        continue
                    birthdate = attributes["IdentityAttributes"].get("birthdate")
                    if birthdate is None:
                        continue

                    self.assertIsInstance(
                        birthdate,
                        dict,
                        f"Identity mapping [{os.path.basename(mapping_path)}] maps "
                        f"birthdate directly, which crashes entity construction on an "
                        f"out-of-bounds source value. Route it through "
                        f"{EXPECTED_BIRTHDATE_FUNCTION_REFERENCE} instead.",
                    )
                    function_reference = birthdate.get("$custom", {}).get("$function")
                    self.assertEqual(
                        EXPECTED_BIRTHDATE_FUNCTION_REFERENCE,
                        function_reference,
                        f"Identity mapping [{os.path.basename(mapping_path)}] must "
                        f"route birthdate through {EXPECTED_BIRTHDATE_FUNCTION_REFERENCE}, "
                        f"found [{function_reference}].",
                    )
