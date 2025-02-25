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
"""Tests for CoreEntity functionality."""

import unittest

import pytest

from recidiviz.persistence.entity.county.entities import Person, Booking, \
    Hold, Charge
from recidiviz.persistence.entity.state import entities
from recidiviz.persistence.database.schema_entity_converter import (
    schema_entity_converter as converter,
)

_STATE_CODE = 'US_ND'


class TestCoreEntity(unittest.TestCase):
    """Tests for CoreEntity functionality."""
    def test_get_entity_name_state_entity_sample(self):
        self.assertEqual(
            'state_person',
            entities.StatePerson.new_with_defaults().get_entity_name())
        self.assertEqual(
            'state_person_race',
            entities.StatePersonRace.new_with_defaults().get_entity_name())
        self.assertEqual(
            'state_court_case',
            entities.StateCourtCase.new_with_defaults().get_entity_name())
        self.assertEqual(
            'state_supervision_violation_response',
            entities.StateSupervisionViolationResponse.new_with_defaults().
            get_entity_name())

    def test_get_entity_name_county_entity_sample(self):
        self.assertEqual('person',
                         Person.new_with_defaults().get_entity_name())
        self.assertEqual('booking',
                         Booking.new_with_defaults().get_entity_name())
        self.assertEqual('hold',
                         Hold.new_with_defaults().get_entity_name())

    def test_get_id_state_entity_sample(self):
        self.assertEqual(
            123,
            entities.StatePerson.new_with_defaults(person_id=123).get_id())
        self.assertEqual(
            456,
            entities.StatePersonRace.new_with_defaults(person_race_id=456)
            .get_id())
        self.assertEqual(
            789,
            entities.StateCourtCase.new_with_defaults(court_case_id=789)
            .get_id())
        self.assertEqual(
            901,
            entities.StateSupervisionViolationResponse.
            new_with_defaults(supervision_violation_response_id=901).get_id())
        self.assertIsNone(entities.StatePerson.new_with_defaults().get_id())

    def test_get_id_county_entity_sample(self):
        self.assertEqual(123,
                         Person.new_with_defaults(person_id=123).get_id())
        self.assertEqual(456,
                         Booking.new_with_defaults(booking_id=456).get_id())
        self.assertEqual(789,
                         Hold.new_with_defaults(hold_id=789).get_id())
        self.assertIsNone(Charge.new_with_defaults().get_id())

    def test_get_class_id_name_state_entity_sample(self):
        self.assertEqual('person_id',
                         entities.StatePerson.get_class_id_name())
        self.assertEqual('fine_id',
                         entities.StateFine.get_class_id_name())
        self.assertEqual('court_case_id',
                         entities.StateCourtCase.get_class_id_name())
        self.assertEqual(
            'supervision_violation_response_id',
            entities.StateSupervisionViolationResponse.get_class_id_name())

    def test_class_id_name_county_entity_sample(self):
        self.assertEqual('person_id',
                         Person.get_class_id_name())
        self.assertEqual('booking_id',
                         Booking.get_class_id_name())
        self.assertEqual('hold_id',
                         Hold.get_class_id_name())

    def test_getField(self):
        entity = entities.StateSentenceGroup.new_with_defaults(
            state_code='us_nc', county_code=None)
        db_entity = converter.convert_entity_to_schema_object(entity)

        self.assertEqual('us_nc', entity.get_field('state_code'))
        self.assertEqual('us_nc', db_entity.get_field('state_code'))

        with pytest.raises(ValueError):
            entity.get_field('country_code')

        with pytest.raises(ValueError):
            db_entity.get_field('country_code')

    def test_getFieldAsList(self):
        fine = entities.StateFine.new_with_defaults(external_id='ex1')
        fine_2 = entities.StateFine.new_with_defaults(external_id='ex2')
        entity = entities.StateSentenceGroup.new_with_defaults(
            state_code='us_nc', fines=[fine, fine_2])
        db_entity = converter.convert_entity_to_schema_object(entity)

        self.assertCountEqual(['us_nc'],
                              entity.get_field_as_list('state_code'))
        self.assertCountEqual([fine, fine_2],
                              entity.get_field_as_list('fines'))

        self.assertCountEqual(['us_nc'],
                              db_entity.get_field_as_list('state_code'))
        self.assertCountEqual([fine, fine_2],
                              converter.convert_schema_objects_to_entity(
                                  db_entity.get_field_as_list('fines'),
                                  populate_back_edges=False))

    def test_setField(self):
        entity = entities.StateSentenceGroup.new_with_defaults()
        db_entity = converter.convert_entity_to_schema_object(entity)

        entity.set_field('state_code', 'us_nc')
        db_entity.set_field('state_code', 'us_nc')

        self.assertEqual('us_nc', entity.state_code)
        self.assertEqual('us_nc', db_entity.state_code)

        with pytest.raises(ValueError):
            entity.set_field('country_code', 'us')

        with pytest.raises(ValueError):
            db_entity.set_field('country_code', 'us')

    def test_setFieldFromList(self):
        entity = entities.StateSentenceGroup.new_with_defaults()
        fine = entities.StateFine.new_with_defaults(external_id='ex1')
        fine_2 = entities.StateFine.new_with_defaults(external_id='ex2')

        db_entity = converter.convert_entity_to_schema_object(entity)
        db_fine = converter.convert_entity_to_schema_object(fine)
        db_fine_2 = converter.convert_entity_to_schema_object(fine_2)

        entity.set_field_from_list('state_code', ['us_nc'])
        self.assertEqual('us_nc', entity.state_code)

        db_entity.set_field_from_list('state_code', ['us_nc'])
        self.assertEqual('us_nc', db_entity.state_code)

        entity.set_field_from_list('fines', [fine, fine_2])
        self.assertCountEqual([fine, fine_2], entity.fines)

        db_entity.set_field_from_list('fines', [db_fine, db_fine_2])
        self.assertCountEqual([fine, fine_2],
                              converter.convert_schema_objects_to_entity(
                                  db_entity.fines,
                                  populate_back_edges=False))

    def test_setFieldFromList_raises(self):
        entity = entities.StateSentenceGroup.new_with_defaults()
        db_entity = converter.convert_entity_to_schema_object(entity)

        with pytest.raises(ValueError):
            entity.set_field_from_list('state_code', ['us_nc', 'us_sc'])

        with pytest.raises(ValueError):
            db_entity.set_field_from_list('state_code', ['us_nc', 'us_sc'])

    def test_hasDefaultStatus(self):
        entity = entities.StateSentenceGroup.new_with_defaults(
            status=entities.StateSentenceStatus.PRESENT_WITHOUT_INFO)
        db_entity = converter.convert_entity_to_schema_object(entity)

        self.assertTrue(entity.has_default_status())
        entity.status = entities.StateSentenceStatus.SERVING
        self.assertFalse(entity.has_default_status())

        self.assertTrue(db_entity.has_default_status())
        db_entity.status = entities.StateSentenceStatus.SERVING.value
        self.assertFalse(db_entity.has_default_status())

    def test_hasDefaultEnum(self):
        entity = entities.StateIncarcerationSentence.new_with_defaults(
            incarceration_type=entities.StateIncarcerationType.STATE_PRISON)
        db_entity = converter.convert_entity_to_schema_object(entity)

        self.assertTrue(
            entity.has_default_enum(
                'incarceration_type',
                entities.StateIncarcerationType.STATE_PRISON))

        entity.incarceration_type_raw_text = 'PRISON'
        self.assertFalse(entity.has_default_enum(
            'incarceration_type', entities.StateIncarcerationType.STATE_PRISON))

        entity.incarceration_type = entities.StateIncarcerationType.COUNTY_JAIL
        entity.incarceration_type_raw_text = None
        self.assertFalse(entity.has_default_enum(
            'incarceration_type', entities.StateIncarcerationType.STATE_PRISON))

        self.assertTrue(
            db_entity.has_default_enum(
                'incarceration_type',
                entities.StateIncarcerationType.STATE_PRISON))

        db_entity.incarceration_type_raw_text = 'PRISON'
        self.assertFalse(db_entity.has_default_enum(
            'incarceration_type', entities.StateIncarcerationType.STATE_PRISON))

        db_entity.incarceration_type = \
            entities.StateIncarcerationType.COUNTY_JAIL.value
        db_entity.incarceration_type_raw_text = None
        self.assertFalse(entity.has_default_enum(
            'incarceration_type', entities.StateIncarcerationType.STATE_PRISON))
