# Recidiviz - a data platform for criminal justice reform
# Copyright (C) 2025 Recidiviz, Inc.
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
"""Unit tests for StateRawFileChunkingMetadataFactory"""
from unittest import TestCase

from recidiviz.ingest.direct.raw_data.state_raw_file_chunking_metadata_factory import (
    StateRawFileChunkingMetadataFactory,
)
from recidiviz.ingest.direct.regions.direct_ingest_region_utils import (
    get_existing_direct_ingest_states,
)


class StateRawFileChunkingMetadataFactoryTest(TestCase):
    """Unit tests for StateRawFileChunkingMetadataFactory"""

    def test_all_regions_have_provider(self) -> None:
        for state_code in get_existing_direct_ingest_states():
            _ = StateRawFileChunkingMetadataFactory.build(region_code=state_code.value)

    def test_unrecognized_region_code_error(self) -> None:
        with self.assertRaisesRegex(
            ValueError, r"Unexpected region code provided: \[US_YY\]"
        ):
            _ = StateRawFileChunkingMetadataFactory.build(region_code="US_YY")
