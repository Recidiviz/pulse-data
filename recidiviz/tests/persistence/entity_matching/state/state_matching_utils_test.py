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

import pytest
from more_itertools import one

from recidiviz.common.constants.charge import ChargeStatus
from recidiviz.common.constants.state.state_incarceration import \
    StateIncarcerationType
from recidiviz.common.constants.state.state_incarceration_period import \
    StateIncarcerationPeriodAdmissionReason, is_revocation_admission
from recidiviz.common.constants.state.state_sentence import StateSentenceStatus
from recidiviz.persistence.database.base_schema import StateBase
from recidiviz.persistence.database.schema.state import schema
from recidiviz.persistence.database.session_factory import SessionFactory
from recidiviz.persistence.entity_matching.state.state_matching_utils import \
    _is_match, generate_child_entity_trees, add_child_to_entity, \
    remove_child_from_entity, \
    get_root_entity_cls, get_total_entities_of_cls, \
    nonnull_fields_entity_match, get_external_ids_of_cls, \
    get_all_entity_trees_of_cls, default_merge_flat_fields, \
    read_persons_by_root_entity_cls, read_db_entity_trees_of_cls_to_merge, \
    read_persons
from recidiviz.persistence.entity.entity_utils import is_placeholder

from recidiviz.persistence.entity_matching.entity_matching_types import \
    EntityTree
from recidiviz.persistence.errors import EntityMatchingError
from recidiviz.tests.persistence.entity_matching.state.base_state_entity_matcher_test_classes import \
    BaseStateMatchingUtilsTest

_EXTERNAL_ID = 'EXTERNAL_ID-1'
_EXTERNAL_ID_2 = 'EXTERNAL_ID-2'
_EXTERNAL_ID_3 = 'EXTERNAL_ID-3'
_ID = 1
_ID_2 = 2
_ID_3 = 3
_COUNTY_CODE = 'COUNTY'
_COUNTY_CODE_ANOTHER = 'COUNTY_ANOTHER'
_STATE_CODE = 'US_XX'
_STATE_CODE_ANOTHER = 'US_XY'
_ID_TYPE = 'ID_TYPE'
_ID_TYPE_ANOTHER = 'ID_TYPE_ANOTHER'


# pylint: disable=protected-access
class TestStateMatchingUtils(BaseStateMatchingUtilsTest):
    """Tests for state entity matching utils"""

    def test_isMatch_statePerson(self) -> None:
        external_id = schema.StatePersonExternalId(
            state_code=_STATE_CODE, external_id=_EXTERNAL_ID)
        external_id_same = schema.StatePersonExternalId(
            state_code=_STATE_CODE, external_id=_EXTERNAL_ID)
        external_id_different = schema.StatePersonExternalId(
            state_code=_STATE_CODE, external_id=_EXTERNAL_ID_2)

        person = schema.StatePerson(
            full_name='name', external_ids=[external_id])
        person_another = schema.StatePerson(
            full_name='name_2', external_ids=[external_id_same])

        self.assertTrue(
            _is_match(ingested_entity=person, db_entity=person_another))
        person_another.external_ids = [external_id_different]
        self.assertFalse(
            _is_match(ingested_entity=person, db_entity=person_another))

    def test_isMatch_statePersonExternalId_type(self) -> None:
        external_id = schema.StatePersonExternalId(
            person_external_id_id=_ID, state_code=_STATE_CODE,
            id_type=_ID_TYPE, external_id=_EXTERNAL_ID)
        external_id_different = schema.StatePersonExternalId(
            person_external_id_id=None, state_code=_STATE_CODE,
            id_type=_ID_TYPE, external_id=_EXTERNAL_ID)
        self.assertTrue(_is_match(ingested_entity=external_id,
                                  db_entity=external_id_different))

        external_id.id_type = _ID_TYPE_ANOTHER
        self.assertFalse(_is_match(ingested_entity=external_id,
                                   db_entity=external_id_different))

    def test_isMatch_statePersonExternalId_externalId(self) -> None:
        external_id = schema.StatePersonExternalId(
            person_external_id_id=_ID, state_code=_STATE_CODE,
            id_type=_ID_TYPE, external_id=_EXTERNAL_ID)
        external_id_different = schema.StatePersonExternalId(
            person_external_id_id=None, state_code=_STATE_CODE,
            id_type=_ID_TYPE, external_id=_EXTERNAL_ID)
        self.assertTrue(_is_match(ingested_entity=external_id,
                                  db_entity=external_id_different))

        external_id.external_id = _EXTERNAL_ID_2
        self.assertFalse(_is_match(ingested_entity=external_id,
                                   db_entity=external_id_different))

    def test_isMatch_statePersonExternalId_stateCode(self) -> None:
        external_id = schema.StatePersonExternalId(
            person_external_id_id=_ID, state_code=_STATE_CODE,
            id_type=_ID_TYPE, external_id=_EXTERNAL_ID)
        external_id_different = schema.StatePersonExternalId(
            person_external_id_id=None, state_code=_STATE_CODE,
            id_type=_ID_TYPE, external_id=_EXTERNAL_ID)
        self.assertTrue(_is_match(ingested_entity=external_id,
                                  db_entity=external_id_different))

        external_id.state_code = _STATE_CODE_ANOTHER
        self.assertFalse(_is_match(ingested_entity=external_id,
                                   db_entity=external_id_different))

    def test_isMatch_statePersonAlias(self) -> None:
        alias = schema.StatePersonAlias(
            state_code=_STATE_CODE, full_name='full_name')
        alias_another = schema.StatePersonAlias(
            state_code=_STATE_CODE, full_name='full_name')
        self.assertTrue(
            _is_match(ingested_entity=alias, db_entity=alias_another))
        alias_another.state_code = _STATE_CODE_ANOTHER
        self.assertFalse(
            _is_match(ingested_entity=alias, db_entity=alias_another))

    def test_isMatch_defaultCompareExternalId(self) -> None:
        charge = schema.StateCharge(
            external_id=_EXTERNAL_ID, description='description')
        charge_another = schema.StateCharge(
            external_id=_EXTERNAL_ID, description='description_another')
        self.assertTrue(
            _is_match(ingested_entity=charge, db_entity=charge_another))
        charge.external_id = _EXTERNAL_ID_2
        self.assertFalse(
            _is_match(ingested_entity=charge, db_entity=charge_another))

    def test_isMatch_defaultCompareNoExternalIds(self) -> None:
        charge = schema.StateCharge()
        charge_another = schema.StateCharge()
        self.assertTrue(
            _is_match(ingested_entity=charge, db_entity=charge_another))
        charge.description = 'description'
        self.assertFalse(
            _is_match(ingested_entity=charge, db_entity=charge_another))

    def test_mergeFlatFields_twoDbEntities(self) -> None:
        to_entity = schema.StateSentenceGroup(
            state_code=_STATE_CODE,
            sentence_group_id=_ID, county_code='county_code',
            status=StateSentenceStatus.SERVING)
        from_entity = schema.StateSentenceGroup(
            state_code=_STATE_CODE,
            sentence_group_id=_ID_2,
            county_code='county_code-updated', max_length_days=10,
            status=StateSentenceStatus.PRESENT_WITHOUT_INFO.value)

        expected_entity = schema.StateSentenceGroup(
            state_code=_STATE_CODE,
            sentence_group_id=_ID,
            county_code='county_code-updated',
            max_length_days=10,
            status=StateSentenceStatus.SERVING.value)

        merged_entity = default_merge_flat_fields(
            new_entity=from_entity, old_entity=to_entity)
        self.assert_schema_objects_equal(expected_entity, merged_entity)

    def test_mergeFlatFields(self) -> None:
        ing_entity = schema.StateSentenceGroup(
            state_code=_STATE_CODE,
            county_code='county_code-updated', max_length_days=10,
            status=StateSentenceStatus.PRESENT_WITHOUT_INFO.value)
        db_entity = schema.StateSentenceGroup(
            state_code=_STATE_CODE,
            sentence_group_id=_ID, county_code='county_code',
            status=StateSentenceStatus.SERVING)
        expected_entity = schema.StateSentenceGroup(
            state_code=_STATE_CODE,
            sentence_group_id=_ID,
            county_code='county_code-updated',
            max_length_days=10,
            status=StateSentenceStatus.SERVING.value)

        merged_entity = default_merge_flat_fields(
            new_entity=ing_entity, old_entity=db_entity)
        self.assert_schema_objects_equal(expected_entity, merged_entity)

    def test_generateChildEntitiesWithAncestorChain(self) -> None:
        fine = schema.StateFine(state_code=_STATE_CODE, fine_id=_ID)
        fine_another = schema.StateFine(state_code=_STATE_CODE, fine_id=_ID_2)
        person = schema.StatePerson(state_code=_STATE_CODE, person_id=_ID)
        sentence_group = schema.StateSentenceGroup(
            status=StateSentenceStatus.PRESENT_WITHOUT_INFO,
            state_code=_STATE_CODE,
            fines=[fine, fine_another],
            person=person, sentence_group_id=_ID)
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

    def test_addChildToEntity(self) -> None:
        fine = schema.StateFine(state_code=_STATE_CODE, fine_id=_ID)
        sentence_group = schema.StateSentenceGroup(
            status=StateSentenceStatus.PRESENT_WITHOUT_INFO,
            state_code=_STATE_CODE,
            fines=[], sentence_group_id=_ID)

        add_child_to_entity(entity=sentence_group,
                            child_field_name='fines',
                            child_to_add=fine)
        self.assertEqual(sentence_group.fines, [fine])

    def test_addChildToEntity_singular(self) -> None:
        charge = schema.StateCharge(state_code=_STATE_CODE,
                                    status=ChargeStatus.PRESENT_WITHOUT_INFO,
                                    charge_id=_ID)
        court_case = schema.StateCourtCase(state_code=_STATE_CODE, court_case_id=_ID)

        add_child_to_entity(entity=charge,
                            child_field_name='court_case',
                            child_to_add=court_case)
        self.assertEqual(charge.court_case, court_case)

    def test_removeChildFromEntity(self) -> None:
        fine = schema.StateFine(state_code=_STATE_CODE, fine_id=_ID)
        fine_another = schema.StateFine(state_code=_STATE_CODE, fine_id=_ID_2)
        sentence_group = schema.StateSentenceGroup(
            status=StateSentenceStatus.PRESENT_WITHOUT_INFO,
            state_code=_STATE_CODE,
            fines=[fine, fine_another],
            sentence_group_id=_ID)

        remove_child_from_entity(
            entity=sentence_group, child_field_name='fines',
            child_to_remove=fine_another)
        self.assertEqual(sentence_group.fines, [fine])

    def test_getRootEntity(self) -> None:
        # Arrange
        incarceration_incident = schema.StateIncarcerationIncident(
            external_id=_EXTERNAL_ID)
        placeholder_incarceration_period = \
            schema.StateIncarcerationPeriod(
                incarceration_incidents=[incarceration_incident])
        placeholder_incarceration_sentence = \
            schema.StateIncarcerationSentence(
                incarceration_periods=[placeholder_incarceration_period])
        placeholder_sentence_group = schema.StateSentenceGroup(
            sentence_group_id=None,
            incarceration_sentences=[placeholder_incarceration_sentence])
        person = schema.StatePerson(
            sentence_groups=[placeholder_sentence_group])

        # Act
        root_entity_cls = get_root_entity_cls([person])

        # Assert
        self.assertEqual(schema.StateIncarcerationIncident,
                         root_entity_cls)

    def test_getRootEntity_emptyList_raises(self) -> None:
        with pytest.raises(EntityMatchingError):
            get_root_entity_cls([])

    def test_getRootEntity_allPlaceholders_raises(self) -> None:
        placeholder_incarceration_period = \
            schema.StateIncarcerationPeriod()
        placeholder_incarceration_sentence = \
            schema.StateIncarcerationSentence(
                incarceration_periods=[placeholder_incarceration_period])
        placeholder_sentence_group = schema.StateSentenceGroup(
            incarceration_sentences=[placeholder_incarceration_sentence])
        placeholder_person = schema.StatePerson(
            sentence_groups=[placeholder_sentence_group])
        with pytest.raises(EntityMatchingError):
            get_root_entity_cls([placeholder_person])

    def test_getAllEntityTreesOfCls(self) -> None:
        sentence_group = schema.StateSentenceGroup(
            sentence_group_id=_ID)
        sentence_group_2 = schema.StateSentenceGroup(
            sentence_group_id=_ID_2)
        person = schema.StatePerson(
            person_id=_ID,
            sentence_groups=[sentence_group, sentence_group_2])

        self.assertEqual(
            [EntityTree(entity=sentence_group, ancestor_chain=[person]),
             EntityTree(entity=sentence_group_2, ancestor_chain=[person])],
            get_all_entity_trees_of_cls([person], schema.StateSentenceGroup))
        self.assertEqual(
            [EntityTree(entity=person, ancestor_chain=[])],
            get_all_entity_trees_of_cls([person], schema.StatePerson))

    def test_getTotalEntitiesOfCls(self) -> None:
        supervision_sentence = schema.StateSupervisionSentence()
        supervision_sentence_2 = schema.StateSupervisionSentence()
        supervision_sentence_3 = schema.StateSupervisionSentence()
        sentence_group = schema.StateSentenceGroup(
            supervision_sentences=[supervision_sentence,
                                   supervision_sentence_2])
        sentence_group_2 = schema.StateSentenceGroup(
            supervision_sentences=[supervision_sentence_2,
                                   supervision_sentence_3])
        person = schema.StatePerson(
            sentence_groups=[sentence_group, sentence_group_2])

        self.assertEqual(3, get_total_entities_of_cls(
            [person], schema.StateSupervisionSentence))
        self.assertEqual(2, get_total_entities_of_cls(
            [person], schema.StateSentenceGroup))
        self.assertEqual(1,
                         get_total_entities_of_cls([person],
                                                   schema.StatePerson))

    def test_getExternalIdsOfCls(self) -> None:
        supervision_sentence = schema.StateSupervisionSentence(
            external_id=_EXTERNAL_ID)
        supervision_sentence_2 = schema.StateSupervisionSentence(
            external_id=_EXTERNAL_ID_2)
        supervision_sentence_3 = schema.StateSupervisionSentence(
            external_id=_EXTERNAL_ID_3)
        sentence_group = schema.StateSentenceGroup(
            external_id=_EXTERNAL_ID,
            supervision_sentences=[supervision_sentence,
                                   supervision_sentence_2])
        sentence_group_2 = schema.StateSentenceGroup(
            external_id=_EXTERNAL_ID_2,
            supervision_sentences=[supervision_sentence_2,
                                   supervision_sentence_3])
        external_id = schema.StatePersonExternalId(
            external_id=_EXTERNAL_ID)
        person = schema.StatePerson(
            external_ids=[external_id],
            sentence_groups=[sentence_group, sentence_group_2])

        self.assertCountEqual(
            [_EXTERNAL_ID, _EXTERNAL_ID_2, _EXTERNAL_ID_3],
            get_external_ids_of_cls([person], schema.StateSupervisionSentence))
        self.assertCountEqual(
            [_EXTERNAL_ID, _EXTERNAL_ID_2],
            get_external_ids_of_cls([person], schema.StateSentenceGroup))
        self.assertCountEqual(
            [_EXTERNAL_ID],
            get_external_ids_of_cls([person], schema.StatePerson))

    def test_getExternalIdsOfCls_emptyExternalId_raises(self) -> None:
        sentence_group = schema.StateSentenceGroup(
            external_id=_EXTERNAL_ID)
        sentence_group_2 = schema.StateSentenceGroup()
        external_id = schema.StatePersonExternalId(
            external_id=_EXTERNAL_ID)
        person = schema.StatePerson(
            external_ids=[external_id],
            sentence_groups=[sentence_group, sentence_group_2])

        with pytest.raises(EntityMatchingError):
            get_external_ids_of_cls([person], schema.StateSentenceGroup)

    def test_getExternalIdsOfCls_emptyPersonExternalId_raises(self) -> None:
        person = schema.StatePerson()
        with pytest.raises(EntityMatchingError):
            get_external_ids_of_cls([person], schema.StatePerson)

    def test_completeEnumSet_admittedForRevocation(self) -> None:
        period = schema.StateIncarcerationPeriod()
        for admission_reason in StateIncarcerationPeriodAdmissionReason:
            period.admission_reason = admission_reason.value
            is_revocation_admission(
                StateIncarcerationPeriodAdmissionReason.parse_from_canonical_string(period.admission_reason))

        is_revocation_admission(StateIncarcerationPeriodAdmissionReason.parse_from_canonical_string(None))

    def test_nonnullFieldsEntityMatch_placeholder(self) -> None:
        charge = schema.StateCharge(state_code=_STATE_CODE, status=ChargeStatus.PRESENT_WITHOUT_INFO)
        charge_another = schema.StateCharge(
            state_code=_STATE_CODE, status=ChargeStatus.PRESENT_WITHOUT_INFO)
        self.assertFalse(
            nonnull_fields_entity_match(
                ingested_entity=EntityTree(entity=charge, ancestor_chain=[]),
                db_entity=EntityTree(entity=charge_another, ancestor_chain=[])))

    def test_nonnullFieldsEntityMatch_externalIdCompare(self) -> None:
        charge = schema.StateCharge(state_code=_STATE_CODE,
                                    status=ChargeStatus.PRESENT_WITHOUT_INFO,
                                    external_id=_EXTERNAL_ID)
        charge_another = schema.StateCharge(
            state_code=_STATE_CODE, status=ChargeStatus.PRESENT_WITHOUT_INFO)
        self.assertFalse(
            nonnull_fields_entity_match(
                ingested_entity=EntityTree(entity=charge, ancestor_chain=[]),
                db_entity=EntityTree(entity=charge_another, ancestor_chain=[])))
        charge_another.external_id = _EXTERNAL_ID
        self.assertTrue(
            nonnull_fields_entity_match(
                ingested_entity=EntityTree(entity=charge, ancestor_chain=[]),
                db_entity=EntityTree(entity=charge_another, ancestor_chain=[])))

    def test_nonnullFieldsEntityMatch_flatFieldsCompare(self) -> None:
        charge = schema.StateCharge(
            state_code=_STATE_CODE,
            ncic_code='1234',
            county_code=_COUNTY_CODE,
            status=ChargeStatus.PRESENT_WITHOUT_INFO)
        charge_another = schema.StateCharge(
            state_code=_STATE_CODE,
            ncic_code='1234',
            status=ChargeStatus.PRESENT_WITHOUT_INFO)

        # If one of the entities is merely missing a field, we still consider it a match
        self.assertTrue(
            nonnull_fields_entity_match(
                ingested_entity=EntityTree(entity=charge, ancestor_chain=[]),
                db_entity=EntityTree(entity=charge_another, ancestor_chain=[])))
        charge_another.county_code = _COUNTY_CODE_ANOTHER

        # If one of the entities has a different value, then it is not a match
        self.assertFalse(
            nonnull_fields_entity_match(
                ingested_entity=EntityTree(entity=charge, ancestor_chain=[]),
                db_entity=EntityTree(entity=charge_another, ancestor_chain=[])))
        charge_another.county_code = _COUNTY_CODE

        # All fields the same - this is a match
        self.assertTrue(
            nonnull_fields_entity_match(
                ingested_entity=EntityTree(entity=charge, ancestor_chain=[]),
                db_entity=EntityTree(entity=charge_another, ancestor_chain=[])))

    def test_readPersons_default(self) -> None:
        schema_person = schema.StatePerson(person_id=1, state_code=_STATE_CODE)
        schema_person_2 = schema.StatePerson(person_id=2, state_code=_STATE_CODE)
        session = SessionFactory.for_schema_base(StateBase)
        session.add(schema_person)
        session.add(schema_person_2)
        session.commit()

        expected_people = [schema_person, schema_person_2]
        people = read_persons(session, _STATE_CODE, [])
        self.assert_schema_object_lists_equal(expected_people, people)

    def test_isPlaceholder(self) -> None:
        entity = schema.StateSentenceGroup(
            status=StateSentenceStatus.PRESENT_WITHOUT_INFO.value,
            state_code=_STATE_CODE,
            fines=[schema.StateFine()],
            person=schema.StatePerson(), sentence_group_id=_ID)
        self.assertTrue(is_placeholder(entity))
        entity.county_code = 'county_code'
        self.assertFalse(is_placeholder(entity))

    def test_isPlaceholder_personWithExternalId(self) -> None:
        sentence_group = schema.StateSentenceGroup(
            state_code=_STATE_CODE, status=StateSentenceStatus.PRESENT_WITHOUT_INFO)
        person = schema.StatePerson(state_code=_STATE_CODE, sentence_groups=[sentence_group])
        self.assertTrue(is_placeholder(person))
        person.external_ids.append(
            schema.StatePersonExternalId(
                state_code=_STATE_CODE, external_id=_EXTERNAL_ID,
                id_type=_ID_TYPE))
        self.assertFalse(is_placeholder(person))

    def test_isPlaceholder_defaultEnumValue(self) -> None:
        entity = schema.StateIncarcerationSentence(
            incarceration_type=StateIncarcerationType.STATE_PRISON.value)
        self.assertTrue(is_placeholder(entity))

        entity.incarceration_type_raw_text = 'PRISON'
        self.assertFalse(is_placeholder(entity))

    def test_readPersonsByRootEntityCls(self) -> None:
        schema_person_with_root_entity = schema.StatePerson(person_id=1, state_code=_STATE_CODE)
        schema_sentence_group = schema.StateSentenceGroup(
            sentence_group_id=_ID,
            external_id=_EXTERNAL_ID,
            status=StateSentenceStatus.PRESENT_WITHOUT_INFO.value,
            state_code=_STATE_CODE)
        schema_sentence_group_2 = schema.StateSentenceGroup(
            sentence_group_id=_ID_2,
            external_id=_EXTERNAL_ID_2,
            status=StateSentenceStatus.PRESENT_WITHOUT_INFO.value,
            state_code=_STATE_CODE)
        schema_person_with_root_entity.sentence_groups = [
            schema_sentence_group, schema_sentence_group_2]
        placeholder_schema_person = schema.StatePerson(person_id=_ID_2, state_code=_STATE_CODE)
        schema_person_other_state = schema.StatePerson(person_id=_ID_3, state_code=_STATE_CODE)
        schema_external_id_other_state = schema.StatePersonExternalId(
            person_external_id_id=_ID_2,
            external_id=_ID,
            id_type=_ID_TYPE,
            state_code=_STATE_CODE)
        schema_person_other_state.external_ids = [
            schema_external_id_other_state]

        session = SessionFactory.for_schema_base(StateBase)
        session.add(schema_person_with_root_entity)
        session.add(placeholder_schema_person)
        session.add(schema_person_other_state)
        session.commit()

        ingested_sentence_group = schema.StateSentenceGroup(
            state_code=_STATE_CODE, external_id=_EXTERNAL_ID)
        ingested_person = schema.StatePerson(
            sentence_groups=[ingested_sentence_group])

        expected_people = [schema_person_with_root_entity,
                           placeholder_schema_person]

        people = read_persons_by_root_entity_cls(
            session, _STATE_CODE, [ingested_person],
            allowed_root_entity_classes=[schema.StateSentenceGroup])
        self.assert_schema_object_lists_equal(expected_people, people)

    def test_readPersons_unexpectedRoot_raises(self) -> None:
        ingested_supervision_sentence = \
            schema.StateSupervisionSentence(
                external_id=_EXTERNAL_ID)
        ingested_sentence_group = schema.StateSentenceGroup(
            supervision_sentences=[ingested_supervision_sentence])
        ingested_person = schema.StatePerson(
            sentence_groups=[ingested_sentence_group])

        with pytest.raises(ValueError):
            session = SessionFactory.for_schema_base(StateBase)
            read_persons_by_root_entity_cls(
                session, 'us_nd', [ingested_person],
                allowed_root_entity_classes=[schema.StateSentenceGroup])

    def test_readDbEntitiesOfClsToMerge(self) -> None:
        person_1 = schema.StatePerson(person_id=1, state_code=_STATE_CODE)
        sentence_group_1 = schema.StateSentenceGroup(
            sentence_group_id=1,
            external_id=_EXTERNAL_ID,
            status=StateSentenceStatus.PRESENT_WITHOUT_INFO.value,
            state_code=_STATE_CODE,
            person=person_1)
        person_1.sentence_groups = [sentence_group_1]

        person_2 = schema.StatePerson(person_id=2, state_code=_STATE_CODE)
        sentence_group_1_dup = schema.StateSentenceGroup(
            sentence_group_id=2,
            external_id=_EXTERNAL_ID,
            status=StateSentenceStatus.PRESENT_WITHOUT_INFO.value,
            state_code=_STATE_CODE,
            person=person_2)
        sentence_group_2 = schema.StateSentenceGroup(
            sentence_group_id=3,
            external_id=_EXTERNAL_ID_2,
            status=StateSentenceStatus.PRESENT_WITHOUT_INFO.value,
            state_code=_STATE_CODE,
            person=person_2)
        placeholder_sentence_group = schema.StateSentenceGroup(
            sentence_group_id=4,
            status=StateSentenceStatus.PRESENT_WITHOUT_INFO.value,
            state_code=_STATE_CODE,
            person=person_2)

        person_2.sentence_groups = [sentence_group_1_dup,
                                    sentence_group_2,
                                    placeholder_sentence_group]

        session = SessionFactory.for_schema_base(StateBase)
        session.add(person_1)
        session.add(person_2)
        session.flush()

        # Act
        trees_to_merge = read_db_entity_trees_of_cls_to_merge(
            session, _STATE_CODE, schema.StateSentenceGroup)

        sentence_group_trees_to_merge = one(trees_to_merge)
        self.assertEqual(len(sentence_group_trees_to_merge), 2)

        sentence_group_ids = set()

        for entity_tree in sentence_group_trees_to_merge:
            self.assertIsInstance(entity_tree, EntityTree)
            entity = entity_tree.entity
            if not isinstance(entity, schema.StateSentenceGroup):
                self.fail(f'Expected StateSentenceGroup. Found {entity}')
            self.assertEqual(entity.external_id, _EXTERNAL_ID)
            sentence_group_ids.add(entity.sentence_group_id)

        self.assertEqual(sentence_group_ids, {1, 2})
