# Recidiviz - a data platform for criminal justice reform
# Copyright (C) 2019 Recidiviz, Inc.
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
"""Tests for base_entity.py"""
from unittest import TestCase

from recidiviz.persistence.entity import base_entity
from recidiviz.persistence.entity.base_entity import Entity
from recidiviz.persistence.entity.county.entities import Person, Booking, \
    Hold, Charge
from recidiviz.persistence.entity.entity_utils import \
    get_all_entity_classes_in_module
from recidiviz.persistence.entity.state.entities import StatePerson, \
    StatePersonRace, StateCourtCase, StateSupervisionViolationResponse


class TestBaseEntities(TestCase):
    """Tests for base_entity.py"""
    def test_base_classes_have_cmp_equal_false(self):
        for entity_class in get_all_entity_classes_in_module(base_entity):
            self.assertEqual(entity_class.__eq__, Entity.__eq__,
                             f"Class [{entity_class}] has an __eq__ function "
                             f"unequal to the base Entity class - did you "
                             f"remember to set cmp=False in the @attr.s "
                             f"declaration?")

    def test_get_entity_name_state_entity_sample(self):
        self.assertEqual('state_person',
                         StatePerson.new_with_defaults().get_entity_name())
        self.assertEqual('state_person_race',
                         StatePersonRace.new_with_defaults().get_entity_name())
        self.assertEqual('state_court_case',
                         StateCourtCase.new_with_defaults().get_entity_name())
        self.assertEqual('state_supervision_violation_response',
                         StateSupervisionViolationResponse.new_with_defaults()
                         .get_entity_name())

    def test_get_entity_name_county_entity_sample(self):
        self.assertEqual('person',
                         Person.new_with_defaults().get_entity_name())
        self.assertEqual('booking',
                         Booking.new_with_defaults().get_entity_name())
        self.assertEqual('hold',
                         Hold.new_with_defaults().get_entity_name())

    def test_get_id_state_entity_sample(self):
        self.assertEqual(123,
                         StatePerson.new_with_defaults(person_id=123).get_id())
        self.assertEqual(456,
                         StatePersonRace.new_with_defaults(person_race_id=456)
                         .get_id())
        self.assertEqual(789,
                         StateCourtCase.new_with_defaults(court_case_id=789)
                         .get_id())
        self.assertEqual(901,
                         StateSupervisionViolationResponse.
                         new_with_defaults(
                             supervision_violation_response_id=901).get_id())
        self.assertIsNone(StatePerson.new_with_defaults().get_id())

    def test_get_id_county_entity_sample(self):
        self.assertEqual(123,
                         Person.new_with_defaults(person_id=123).get_id())
        self.assertEqual(456,
                         Booking.new_with_defaults(booking_id=456).get_id())
        self.assertEqual(789,
                         Hold.new_with_defaults(hold_id=789).get_id())
        self.assertIsNone(Charge.new_with_defaults().get_id())

    # TODO(1625): Write unit tests for entity graph equality that reference the
    # schema defined in test_schema/test_entities.py.
