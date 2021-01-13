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
"""Tests for deserialize_entity_factories.py."""
import unittest
from datetime import date

from recidiviz.common.constants.enum_overrides import EnumOverrides
from recidiviz.common.constants.enum_parser import EnumParser
from recidiviz.common.constants.person_characteristics import Gender, ResidencyStatus
from recidiviz.persistence.entity.state import deserialize_entity_factories, entities


class TestDeserializeEntityFactories(unittest.TestCase):
    """Tests for deserialize_entity_factories.py."""

    def test_deserialize_StatePerson(self) -> None:
        result = deserialize_entity_factories.StatePersonFactory.deserialize(
            state_code='us_xx',
            gender=EnumParser('MALE', Gender, EnumOverrides.empty()),
            gender_raw_text='MALE',
            full_name='{"full_name": "full NAME"}',
            birthdate='12-31-1999',
            current_address='NNN\n  STREET \t ZIP',
            residency_status='NNN\n  STREET \t ZIP'
        )

        # Assert
        expected_result = entities.StatePerson.new_with_defaults(
            gender=Gender.MALE,
            gender_raw_text='MALE',
            full_name='{"full_name": "FULL NAME"}',
            birthdate=date(year=1999, month=12, day=31),
            birthdate_inferred_from_age=None,
            current_address='NNN STREET ZIP',
            residency_status=ResidencyStatus.PERMANENT,
            state_code='US_XX'
        )

        self.assertEqual(result, expected_result)

    def test_deserialize_StatePersonExternalId(self) -> None:
        result = deserialize_entity_factories.StatePersonExternalIdFactory.deserialize(
            external_id='123a',
            id_type='state_id',
            state_code='us_xx',
        )

        # Assert
        expected_result = entities.StatePersonExternalId(
            external_id='123A',
            id_type='STATE_ID',
            state_code='US_XX',
        )

        self.assertEqual(result, expected_result)
