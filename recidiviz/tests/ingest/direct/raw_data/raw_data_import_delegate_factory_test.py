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
"""Unit tests for RawDataImportDelegateFactory"""


from unittest import TestCase

from recidiviz.ingest.direct.raw_data.raw_data_import_delegate_factory import (
    RawDataImportDelegateFactory,
)
from recidiviz.ingest.direct.regions.direct_ingest_region_utils import (
    get_existing_direct_ingest_states,
)


class RawDataImportDelegateFactoryTest(TestCase):
    """Unit tests for RawDataImportDelegateFactory"""

    def test_all_regions_have_delegate(self) -> None:
        for state_code in get_existing_direct_ingest_states():
            _ = RawDataImportDelegateFactory.build(region_code=state_code.value)

        with self.assertRaisesRegex(
            ValueError, r"Unexpected region code provided: \[THIS ISNT A REGION CODE\]"
        ):
            _ = RawDataImportDelegateFactory.build(
                region_code="this isnt a region code"
            )
