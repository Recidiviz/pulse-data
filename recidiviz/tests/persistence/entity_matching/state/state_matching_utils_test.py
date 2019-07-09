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
"""Tests for state_matching_utils.py"""
import datetime
from unittest import TestCase

import attr

from recidiviz.common.constants.state.state_incarceration_period import \
    StateIncarcerationPeriodStatus, StateIncarcerationPeriodAdmissionReason, \
    StateIncarcerationPeriodReleaseReason
from recidiviz.common.constants.state.state_sentence import StateSentenceStatus
from recidiviz.persistence.entity.state.entities import StatePersonExternalId, \
    StatePerson, StatePersonAlias, StateCharge, StateSentenceGroup, StateFine, \
    StateIncarcerationPeriod
from recidiviz.persistence.entity_matching.state.state_matching_utils import \
    merge_flat_fields, remove_back_edges, add_person_to_entity_graph, \
    is_placeholder, _is_match, EntityTree, \
    generate_child_entity_trees, add_child_to_entity, \
    remove_child_from_entity, _merge_incarceration_periods_helper, \
    has_default_status

_DATE_1 = datetime.date(year=2019, month=1, day=1)
_DATE_2 = datetime.date(year=2019, month=2, day=1)
_DATE_3 = datetime.date(year=2019, month=3, day=1)
_DATE_4 = datetime.date(year=2019, month=4, day=1)
_DATE_5 = datetime.date(year=2019, month=5, day=1)
_EXTERNAL_ID = 'EXTERNAL_ID'
_EXTERNAL_ID_2 = 'EXTERNAL_ID_2'
_EXTERNAL_ID_3 = 'EXTERNAL_ID_3'
_EXTERNAL_ID_4 = 'EXTERNAL_ID_4'
_EXTERNAL_ID_5 = 'EXTERNAL_ID_5'
_EXTERNAL_ID_6 = 'EXTERNAL_ID_6'
_ID = 1
_ID_2 = 2
_ID_3 = 3
_STATE_CODE = 'NC'
_STATE_CODE_ANOTHER = 'CA'
_FULL_NAME = 'NAME'
_ID_TYPE = 'ID_TYPE'
_ID_TYPE_ANOTHER = 'ID_TYPE_ANOTHER'
_FACILITY = 'FACILITY'
_FACILITY_2 = 'FACILITY_2'
_FACILITY_3 = 'FACILITY_3'


# pylint: disable=protected-access
class TestStateMatchingUtils(TestCase):
    """Tests for state entity matching utils"""

    def test_isMatch_statePerson(self):
        external_id = StatePersonExternalId.new_with_defaults(
            state_code=_STATE_CODE, external_id=_EXTERNAL_ID)
        external_id_different = attr.evolve(
            external_id, external_id=_EXTERNAL_ID_2)
        person = StatePerson.new_with_defaults(
            full_name='name', external_ids=[external_id])
        person_another = StatePerson.new_with_defaults(
            full_name='name_2', external_ids=[external_id])

        self.assertTrue(
            _is_match(ingested_entity=person, db_entity=person_another))
        person_another.external_ids = [external_id_different]
        self.assertFalse(
            _is_match(ingested_entity=person, db_entity=person_another))

    def test_isMatch_statePersonExternalId_type(self):
        external_id = StatePersonExternalId.new_with_defaults(
            person_external_id_id=_ID, state_code=_STATE_CODE,
            id_type=_ID_TYPE, external_id=_EXTERNAL_ID)
        external_id_different = attr.evolve(
            external_id, person_external_id_id=None)
        self.assertTrue(_is_match(ingested_entity=external_id,
                                  db_entity=external_id_different))

        external_id.id_type = _ID_TYPE_ANOTHER
        self.assertFalse(_is_match(ingested_entity=external_id,
                                   db_entity=external_id_different))

    def test_isMatch_statePersonExternalId_externalId(self):
        external_id = StatePersonExternalId.new_with_defaults(
            person_external_id_id=_ID, state_code=_STATE_CODE,
            id_type=_ID_TYPE, external_id=_EXTERNAL_ID)
        external_id_different = attr.evolve(
            external_id, person_external_id_id=None)
        self.assertTrue(_is_match(ingested_entity=external_id,
                                  db_entity=external_id_different))

        external_id.external_id = _EXTERNAL_ID_2
        self.assertFalse(_is_match(ingested_entity=external_id,
                                   db_entity=external_id_different))

    def test_isMatch_statePersonExternalId_stateCode(self):
        external_id = StatePersonExternalId.new_with_defaults(
            person_external_id_id=_ID, state_code=_STATE_CODE,
            id_type=_ID_TYPE, external_id=_EXTERNAL_ID)
        external_id_different = attr.evolve(
            external_id, person_external_id_id=None)
        self.assertTrue(_is_match(ingested_entity=external_id,
                                  db_entity=external_id_different))

        external_id.state_code = _STATE_CODE_ANOTHER
        self.assertFalse(_is_match(ingested_entity=external_id,
                                   db_entity=external_id_different))

    def test_isMatch_statePersonAlias(self):
        alias = StatePersonAlias.new_with_defaults(
            state_code=_STATE_CODE, full_name='full_name')
        alias_another = attr.evolve(alias, full_name='full_name_2')
        self.assertTrue(
            _is_match(ingested_entity=alias, db_entity=alias_another))
        alias_another.state_code = _STATE_CODE_ANOTHER
        self.assertFalse(
            _is_match(ingested_entity=alias, db_entity=alias_another))

    def test_isMatch_defaultCompareExternalId(self):
        charge = StateCharge.new_with_defaults(
            external_id=_EXTERNAL_ID, description='description')
        charge_another = attr.evolve(charge, description='description_another')
        self.assertTrue(
            _is_match(ingested_entity=charge, db_entity=charge_another))
        charge.external_id = _EXTERNAL_ID_2
        self.assertFalse(
            _is_match(ingested_entity=charge, db_entity=charge_another))

    def test_hasDefaultStatus(self):
        entity = StateSentenceGroup.new_with_defaults(
            status=StateSentenceStatus.PRESENT_WITHOUT_INFO)
        self.assertTrue(has_default_status(entity))
        entity.status = StateSentenceStatus.SERVING
        self.assertFalse(has_default_status(entity))

    def test_mergeFlatFields(self):
        ing_entity = StateSentenceGroup.new_with_defaults(
            county_code='county_code-updated', max_length_days=10,
            status=StateSentenceStatus.PRESENT_WITHOUT_INFO)
        db_entity = StateSentenceGroup.new_with_defaults(
            sentence_group_id=_ID, county_code='county_code',
            status=StateSentenceStatus.SERVING)
        expected_entity = attr.evolve(ing_entity, sentence_group_id=_ID,
                                      status=StateSentenceStatus.SERVING)

        self.assertEqual(
            expected_entity,
            merge_flat_fields(new_entity=ing_entity, old_entity=db_entity))

    def test_removeBackedges(self):
        person = StatePerson.new_with_defaults(full_name=_FULL_NAME)
        sentence_group = StateSentenceGroup.new_with_defaults(
            external_id=_EXTERNAL_ID, person=person)
        fine = StateFine.new_with_defaults(
            external_id=_EXTERNAL_ID,
            sentence_group=sentence_group, person=person)

        sentence_group.fines = [fine]
        person.sentence_groups = [sentence_group]

        expected_fine = attr.evolve(
            fine, sentence_group=None, person=None)
        expected_sentence_group = attr.evolve(
            sentence_group, person=None, fines=[expected_fine])
        expected_person = attr.evolve(
            person, sentence_groups=[expected_sentence_group])

        remove_back_edges(person)
        self.assertEqual(expected_person, person)

    def test_addPersonToEntityTree(self):
        fine = StateFine.new_with_defaults(external_id=_EXTERNAL_ID)
        sentence_group = StateSentenceGroup.new_with_defaults(
            external_id=_EXTERNAL_ID,
            fines=[fine])
        person = StatePerson.new_with_defaults(
            full_name=_FULL_NAME,
            sentence_groups=[sentence_group])

        expected_person = attr.evolve(person)
        expected_fine = attr.evolve(fine, person=expected_person)
        expected_sentence_group = attr.evolve(
            sentence_group, person=expected_person, fines=[expected_fine])
        expected_person.sentence_groups = [expected_sentence_group]

        add_person_to_entity_graph([person])
        self.assertEqual(expected_person, person)

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

    def test_generateChildEntitiesWithAncestorChain(self):
        fine = StateFine.new_with_defaults(fine_id=_ID)
        fine_another = StateFine.new_with_defaults(fine_id=_ID_2)
        person = StatePerson.new_with_defaults(person_id=_ID)
        sentence_group = StateSentenceGroup.new_with_defaults(
            status=StateSentenceStatus.PRESENT_WITHOUT_INFO,
            state_code=_STATE_CODE,
            fines=[fine, fine_another],
            person=[person], sentence_group_id=_ID)
        sentence_group_tree = EntityTree(
            entity=sentence_group, ancestor_chain=[person])

        expected_child_trees = [
            EntityTree(
                entity=fine, ancestor_chain=[person, sentence_group]),
            EntityTree(
                entity=fine_another, ancestor_chain=[person, sentence_group]),
        ]

        self.assertEqual(
            expected_child_trees,
            generate_child_entity_trees(
                'fines', [sentence_group_tree]))

    def test_addChildToEntity(self):
        fine = StateFine.new_with_defaults(fine_id=_ID)
        sentence_group = StateSentenceGroup.new_with_defaults(
            status=StateSentenceStatus.PRESENT_WITHOUT_INFO,
            state_code=_STATE_CODE,
            fines=[], sentence_group_id=_ID)

        expected_sentence_group = attr.evolve(
            sentence_group, fines=[fine])
        add_child_to_entity(entity=sentence_group,
                            child_field_name='fines',
                            child_to_add=fine)
        self.assertEqual(expected_sentence_group, sentence_group)

    def test_removeChildFromEntity(self):
        fine = StateFine.new_with_defaults(fine_id=_ID)
        fine_another = StateFine.new_with_defaults(fine_id=_ID_2)
        sentence_group = StateSentenceGroup.new_with_defaults(
            status=StateSentenceStatus.PRESENT_WITHOUT_INFO,
            state_code=_STATE_CODE,
            fines=[fine, fine_another],
            sentence_group_id=_ID)

        expected_sentence_group = attr.evolve(sentence_group, fines=[fine])
        remove_child_from_entity(
            entity=sentence_group, child_field_name='fines',
            child_to_remove=fine_another)
        self.assertEqual(expected_sentence_group, sentence_group)

    def test_mergeIncarcerationPeriods(self):
        incarceration_period_1 = StateIncarcerationPeriod.new_with_defaults(
            external_id=_EXTERNAL_ID,
            status=StateIncarcerationPeriodStatus.IN_CUSTODY,
            facility=_FACILITY, admission_date=_DATE_1,
            admission_reason=
            StateIncarcerationPeriodAdmissionReason.NEW_ADMISSION)
        incarceration_period_2 = StateIncarcerationPeriod.new_with_defaults(
            external_id=_EXTERNAL_ID_2,
            status=StateIncarcerationPeriodStatus.IN_CUSTODY,
            facility=_FACILITY, release_date=_DATE_2,
            release_reason=StateIncarcerationPeriodReleaseReason.TRANSFER)
        incarceration_period_3 = StateIncarcerationPeriod.new_with_defaults(
            external_id=_EXTERNAL_ID_3,
            status=StateIncarcerationPeriodStatus.IN_CUSTODY,
            facility=_FACILITY_2, admission_date=_DATE_2,
            admission_reason=
            StateIncarcerationPeriodAdmissionReason.TRANSFER)
        incarceration_period_4 = StateIncarcerationPeriod.new_with_defaults(
            external_id=_EXTERNAL_ID_4,
            status=StateIncarcerationPeriodStatus.IN_CUSTODY,
            facility=_FACILITY_2, release_date=_DATE_3,
            release_reason=StateIncarcerationPeriodReleaseReason.TRANSFER)
        incarceration_period_5 = StateIncarcerationPeriod.new_with_defaults(
            external_id=_EXTERNAL_ID_5,
            status=StateIncarcerationPeriodStatus.IN_CUSTODY,
            facility=_FACILITY, admission_date=_DATE_4,
            admission_reason=
            StateIncarcerationPeriodAdmissionReason.TRANSFER)
        incarceration_period_6 = StateIncarcerationPeriod.new_with_defaults(
            external_id=_EXTERNAL_ID_6,
            status=StateIncarcerationPeriodStatus.IN_CUSTODY,
            facility=_FACILITY_3, release_date=_DATE_5,
            release_reason=StateIncarcerationPeriodReleaseReason.TRANSFER)

        expected_merged_incarceration_period_1 = \
            StateIncarcerationPeriod.new_with_defaults(
                external_id=_EXTERNAL_ID + '|' + _EXTERNAL_ID_2,
                status=StateIncarcerationPeriodStatus.NOT_IN_CUSTODY,
                facility=_FACILITY, admission_date=_DATE_1,
                admission_reason=
                StateIncarcerationPeriodAdmissionReason.NEW_ADMISSION,
                release_date=_DATE_2,
                release_reason=StateIncarcerationPeriodReleaseReason.TRANSFER)
        expected_merged_incarceration_period_2 = \
            StateIncarcerationPeriod.new_with_defaults(
                external_id=_EXTERNAL_ID_3 + '|' + _EXTERNAL_ID_4,
                status=StateIncarcerationPeriodStatus.NOT_IN_CUSTODY,
                facility=_FACILITY_2, admission_date=_DATE_2,
                admission_reason=
                StateIncarcerationPeriodAdmissionReason.TRANSFER,
                release_date=_DATE_3,
                release_reason=StateIncarcerationPeriodReleaseReason.TRANSFER)
        expected_unmerged_incarceration_period = attr.evolve(
            incarceration_period_5)
        expected_unmerged_incarceration_period_another = attr.evolve(
            incarceration_period_6)

        expected_incarceration_periods = [
            expected_merged_incarceration_period_1,
            expected_merged_incarceration_period_2,
            expected_unmerged_incarceration_period,
            expected_unmerged_incarceration_period_another]

        ingested_incarceration_periods = [
            incarceration_period_1, incarceration_period_5,
            incarceration_period_2, incarceration_period_4,
            incarceration_period_3, incarceration_period_6
        ]

        merged_periods = _merge_incarceration_periods_helper(
            ingested_incarceration_periods)

        self.assertCountEqual(expected_incarceration_periods, merged_periods)

    def test_mergeIncarcerationPeriods_multipleTransfersSameDate(self):
        incarceration_period_1 = StateIncarcerationPeriod.new_with_defaults(
            external_id=_EXTERNAL_ID,
            status=StateIncarcerationPeriodStatus.IN_CUSTODY,
            facility=_FACILITY, admission_date=_DATE_1,
            admission_reason=
            StateIncarcerationPeriodAdmissionReason.NEW_ADMISSION)
        incarceration_period_2 = StateIncarcerationPeriod.new_with_defaults(
            external_id=_EXTERNAL_ID_2,
            status=StateIncarcerationPeriodStatus.IN_CUSTODY,
            facility=_FACILITY, release_date=_DATE_2,
            release_reason=StateIncarcerationPeriodReleaseReason.TRANSFER)
        incarceration_period_3 = StateIncarcerationPeriod.new_with_defaults(
            external_id=_EXTERNAL_ID_3,
            status=StateIncarcerationPeriodStatus.IN_CUSTODY,
            facility=_FACILITY_2, admission_date=_DATE_2,
            admission_reason=
            StateIncarcerationPeriodAdmissionReason.TRANSFER)
        incarceration_period_4 = StateIncarcerationPeriod.new_with_defaults(
            external_id=_EXTERNAL_ID_4,
            status=StateIncarcerationPeriodStatus.IN_CUSTODY,
            facility=_FACILITY_2, release_date=_DATE_2,
            release_reason=StateIncarcerationPeriodReleaseReason.TRANSFER)
        incarceration_period_5 = StateIncarcerationPeriod.new_with_defaults(
            external_id=_EXTERNAL_ID_5,
            status=StateIncarcerationPeriodStatus.IN_CUSTODY,
            facility=_FACILITY_3, admission_date=_DATE_2,
            admission_reason=
            StateIncarcerationPeriodAdmissionReason.TRANSFER)
        incarceration_period_6 = StateIncarcerationPeriod.new_with_defaults(
            external_id=_EXTERNAL_ID_6,
            status=StateIncarcerationPeriodStatus.IN_CUSTODY,
            facility=_FACILITY_3, release_date=_DATE_2,
            release_reason=
            StateIncarcerationPeriodReleaseReason.SENTENCE_SERVED)

        expected_merged_incarceration_period_1 = \
            StateIncarcerationPeriod.new_with_defaults(
                external_id=_EXTERNAL_ID + '|' + _EXTERNAL_ID_2,
                status=StateIncarcerationPeriodStatus.NOT_IN_CUSTODY,
                facility=_FACILITY, admission_date=_DATE_1,
                admission_reason=
                StateIncarcerationPeriodAdmissionReason.NEW_ADMISSION,
                release_date=_DATE_2,
                release_reason=StateIncarcerationPeriodReleaseReason.TRANSFER)
        expected_merged_incarceration_period_2 = \
            StateIncarcerationPeriod.new_with_defaults(
                external_id=_EXTERNAL_ID_3 + '|' + _EXTERNAL_ID_4,
                status=StateIncarcerationPeriodStatus.NOT_IN_CUSTODY,
                facility=_FACILITY_2, admission_date=_DATE_2,
                admission_reason=
                StateIncarcerationPeriodAdmissionReason.TRANSFER,
                release_date=_DATE_2,
                release_reason=StateIncarcerationPeriodReleaseReason.TRANSFER)
        expected_merged_incarceration_period_3 = \
            StateIncarcerationPeriod.new_with_defaults(
                external_id=_EXTERNAL_ID_5 + '|' + _EXTERNAL_ID_6,
                status=StateIncarcerationPeriodStatus.NOT_IN_CUSTODY,
                facility=_FACILITY_3, admission_date=_DATE_2,
                admission_reason=
                StateIncarcerationPeriodAdmissionReason.TRANSFER,
                release_date=_DATE_2,
                release_reason=
                StateIncarcerationPeriodReleaseReason.SENTENCE_SERVED)

        expected_incarceration_periods = [
            expected_merged_incarceration_period_1,
            expected_merged_incarceration_period_2,
            expected_merged_incarceration_period_3]

        ingested_incarceration_periods = [
            incarceration_period_1, incarceration_period_5,
            incarceration_period_2, incarceration_period_4,
            incarceration_period_3, incarceration_period_6
        ]

        merged_periods = _merge_incarceration_periods_helper(
            ingested_incarceration_periods)

        self.assertCountEqual(expected_incarceration_periods, merged_periods)
