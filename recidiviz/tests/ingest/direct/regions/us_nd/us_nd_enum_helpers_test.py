# Recidiviz - a data platform for criminal justice reform
# Copyright (C) 2021 Recidiviz, Inc.
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
"""Tests the functions in the us_nd_enum_helpers file."""
import unittest

from recidiviz.common.constants.state.state_supervision_contact import (
    StateSupervisionContactLocation,
    StateSupervisionContactStatus,
    StateSupervisionContactType,
)
from recidiviz.common.constants.states import StateCode
from recidiviz.ingest.direct.regions.us_nd.us_nd_enum_helpers import (
    supervision_contact_location_mapper,
    supervision_contact_status_mapper,
    supervision_contact_type_mapper,
)
from recidiviz.tests.ingest.direct.fixture_util import direct_ingest_fixture_path


class TestUsNdEnumMapperFunctions(unittest.TestCase):
    """Tests the mapper functions from the us_nd_enum_helpers file
    which parse raw text from the incarceration period ingest view"""

    def setUp(self) -> None:
        self.region_code = StateCode.US_ND.value.lower()

    def test_supervision_contact_field_mappers(self) -> None:
        fixture_path = direct_ingest_fixture_path(
            region_code=self.region_code,
            file_name="docstars_contacts_location_status_raw_text.csv",
        )
        with open(fixture_path, "r", encoding="utf-8") as f:
            while True:
                contact_str = f.readline().strip()
                if not contact_str:
                    break
                contact_type_mapping = supervision_contact_type_mapper(contact_str)
                self.assertIsNotNone(contact_type_mapping)
                self.assertIsInstance(contact_type_mapping, StateSupervisionContactType)
                contact_location_mapping = supervision_contact_location_mapper(
                    contact_str
                )
                self.assertIsNotNone(contact_location_mapping)
                self.assertIsInstance(
                    contact_location_mapping, StateSupervisionContactLocation
                )
                contact_status_mapping = supervision_contact_status_mapper(contact_str)
                self.assertIsNotNone(contact_status_mapping)
                self.assertIsInstance(
                    contact_status_mapping, StateSupervisionContactStatus
                )
