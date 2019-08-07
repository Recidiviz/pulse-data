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
"""Tests for entity_utils.py"""
from unittest import TestCase

import pytest

from recidiviz.common.constants.state.state_incarceration import \
    StateIncarcerationType
from recidiviz.common.constants.state.state_sentence import StateSentenceStatus
from recidiviz.persistence.entity.entity_utils import \
    get_set_entity_field_names, EntityFieldType, get_field, set_field, \
    get_field_as_list, set_field_from_list, is_placeholder, \
    has_default_status, has_default_enum
from recidiviz.persistence.entity.state.entities import StateSentenceGroup, \
    StateFine, StatePerson, StatePersonExternalId, StateIncarcerationSentence
from recidiviz.persistence.errors import EntityMatchingError

_ID = 1
_STATE_CODE = 'NC'
_EXTERNAL_ID = 'EXTERNAL_ID-1'
_ID_TYPE = 'ID_TYPE'


class TestEntityUtils(TestCase):
    """Tests the functionality of our entity utils."""

    def test_getEntityRelationshipFieldNames_children(self):
        entity = StateSentenceGroup.new_with_defaults(
            fines=[StateFine.new_with_defaults()],
            person=[StatePerson.new_with_defaults()], sentence_group_id=_ID)
        self.assertEqual(
            {'fines'},
            get_set_entity_field_names(entity, EntityFieldType.FORWARD_EDGE))

    def test_getEntityRelationshipFieldNames_backedges(self):
        entity = StateSentenceGroup.new_with_defaults(
            fines=[StateFine.new_with_defaults()],
            person=[StatePerson.new_with_defaults()], sentence_group_id=_ID)
        self.assertEqual(
            {'person'},
            get_set_entity_field_names(entity, EntityFieldType.BACK_EDGE))

    def test_getEntityRelationshipFieldNames_flatFields(self):
        entity = StateSentenceGroup.new_with_defaults(
            fines=[StateFine.new_with_defaults()],
            person=[StatePerson.new_with_defaults()], sentence_group_id=_ID)
        self.assertEqual(
            {'sentence_group_id'},
            get_set_entity_field_names(entity, EntityFieldType.FLAT_FIELD))

    def test_getField(self):
        entity = StateSentenceGroup.new_with_defaults(
            state_code='us_nc', county_code=None)
        self.assertEqual('us_nc', get_field(entity, 'state_code'))
        with pytest.raises(EntityMatchingError):
            get_field(entity, 'country_code')

    def test_getFieldAsList(self):
        fine = StateFine.new_with_defaults(external_id='ex1')
        fine_2 = StateFine.new_with_defaults(external_id='ex2')
        entity = StateSentenceGroup.new_with_defaults(
            state_code='us_nc', fines=[fine, fine_2])
        self.assertCountEqual(['us_nc'],
                              get_field_as_list(entity, 'state_code'))
        self.assertCountEqual([fine, fine_2],
                              get_field_as_list(entity, 'fines'))

    def test_setField(self):
        entity = StateSentenceGroup.new_with_defaults()
        set_field(entity, 'state_code', 'us_nc')
        self.assertEqual('us_nc', entity.state_code)
        with pytest.raises(EntityMatchingError):
            set_field(entity, 'country_code', 'us')

    def test_setFieldFromList(self):
        entity = StateSentenceGroup.new_with_defaults()
        fine = StateFine.new_with_defaults(external_id='ex1')
        fine_2 = StateFine.new_with_defaults(external_id='ex2')
        set_field_from_list(entity, 'state_code', ['us_nc'])
        self.assertEqual('us_nc', entity.state_code)
        set_field_from_list(entity, 'fines', [fine, fine_2])
        self.assertCountEqual([fine, fine_2], entity.fines)

    def test_setFieldFromList_raises(self):
        entity = StateSentenceGroup.new_with_defaults()
        with pytest.raises(EntityMatchingError):
            set_field_from_list(entity, 'state_code', ['us_nc', 'us_sc'])

    def test_isPlaceholder(self):
        entity = StateSentenceGroup.new_with_defaults(
            status=StateSentenceStatus.PRESENT_WITHOUT_INFO,
            state_code=_STATE_CODE,
            fines=[StateFine.new_with_defaults()],
            person=[StatePerson.new_with_defaults()], sentence_group_id=_ID)
        self.assertTrue(is_placeholder(entity))
        entity.county_code = 'county_code'
        self.assertFalse(is_placeholder(entity))

    def test_isPlaceholder_personWithExternalId(self):
        sentence_group = StateSentenceGroup.new_with_defaults(
            state_code=_STATE_CODE)
        person = StatePerson.new_with_defaults(sentence_groups=[sentence_group])
        self.assertTrue(is_placeholder(person))
        person.external_ids.append(
            StatePersonExternalId.new_with_defaults(
                state_code=_STATE_CODE, external_id=_EXTERNAL_ID,
                id_type=_ID_TYPE))
        self.assertFalse(is_placeholder(person))

    def test_isPlaceholder_defaultEnumValue(self):
        entity = StateIncarcerationSentence.new_with_defaults(
            incarceration_type=StateIncarcerationType.STATE_PRISON)
        self.assertTrue(is_placeholder(entity))

        entity.incarceration_type_raw_text = 'PRISON'
        self.assertFalse(is_placeholder(entity))

    def test_hasDefaultStatus(self):
        entity = StateSentenceGroup.new_with_defaults(
            status=StateSentenceStatus.PRESENT_WITHOUT_INFO)
        self.assertTrue(has_default_status(entity))
        entity.status = StateSentenceStatus.SERVING
        self.assertFalse(has_default_status(entity))

    def test_hasDefaultEnum(self):
        entity = StateIncarcerationSentence.new_with_defaults(
            incarceration_type=StateIncarcerationType.STATE_PRISON)
        self.assertTrue(has_default_enum(entity, 'incarceration_type',
                                         StateIncarcerationType.STATE_PRISON))

        entity.incarceration_type_raw_text = 'PRISON'
        self.assertFalse(has_default_enum(entity, 'incarceration_type',
                                          StateIncarcerationType.STATE_PRISON))

        entity.incarceration_type = StateIncarcerationType.COUNTY_JAIL
        entity.incarceration_type_raw_text = None
        self.assertFalse(has_default_enum(entity, 'incarceration_type',
                                          StateIncarcerationType.STATE_PRISON))
