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
"""Tests for entity_merger_utils.py"""
import unittest

from recidiviz.common.constants.state.state_person import StateRace
from recidiviz.persistence.entity_matching.entity_merger_utils import (
    enum_entity_key,
    external_id_key,
    root_entity_external_id_keys,
)
from recidiviz.tests.persistence.entity_matching.us_xx_entity_builders import (
    make_person,
    make_person_external_id,
    make_person_race,
    make_staff_external_id,
)


class TestEntityMergerUtils(unittest.TestCase):
    """Tests for entity_merger_utils.py"""

    def test_external_id_key(self) -> None:
        person_external_id = make_person_external_id(
            external_id="ID1", id_type="US_XX_ID_TYPE"
        )

        self.assertEqual(
            "StatePersonExternalId##US_XX_ID_TYPE|ID1",
            external_id_key(person_external_id),
        )

        staff_external_id = make_staff_external_id(
            external_id="ID1", id_type="US_XX_ID_TYPE"
        )

        self.assertEqual(
            "StateStaffExternalId##US_XX_ID_TYPE|ID1",
            external_id_key(staff_external_id),
        )

    def test_root_entity_external_id_keys(self) -> None:
        person = make_person(
            external_ids=[
                make_person_external_id(external_id="ID1", id_type="US_XX_ID_TYPE"),
                make_person_external_id(external_id="ID2", id_type="US_XX_ID_TYPE_2"),
            ],
        )

        self.assertEqual(
            {
                "StatePersonExternalId##US_XX_ID_TYPE|ID1",
                "StatePersonExternalId##US_XX_ID_TYPE_2|ID2",
            },
            root_entity_external_id_keys(person),
        )

    def test_enum_entity_key(self) -> None:
        person_race = make_person_race(race=StateRace.BLACK, race_raw_text="B")

        self.assertEqual(
            "StatePersonRace##StateRace.BLACK|B",
            enum_entity_key(person_race),
        )
