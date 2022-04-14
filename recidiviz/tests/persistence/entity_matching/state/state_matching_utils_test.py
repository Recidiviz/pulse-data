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

from recidiviz.common.constants.shared_enums.person_characteristics import Gender
from recidiviz.common.constants.state.state_charge import StateChargeStatus
from recidiviz.common.constants.state.state_incarceration import StateIncarcerationType
from recidiviz.common.constants.state.state_incarceration_period import (
    StateIncarcerationPeriodAdmissionReason,
    is_commitment_from_supervision,
)
from recidiviz.common.constants.state.state_sentence import StateSentenceStatus
from recidiviz.persistence.database.database_entity import DatabaseEntity
from recidiviz.persistence.database.schema.state import schema
from recidiviz.persistence.database.schema_entity_converter.state.schema_entity_converter import (
    StateSchemaToEntityConverter,
)
from recidiviz.persistence.database.schema_utils import (
    get_non_history_state_database_entities,
)
from recidiviz.persistence.entity.base_entity import Entity
from recidiviz.persistence.entity.entity_utils import (
    CoreEntityFieldIndex,
    is_placeholder,
)
from recidiviz.persistence.entity_matching.entity_matching_types import EntityTree
from recidiviz.persistence.entity_matching.state.state_matching_utils import (
    _is_match,
    add_child_to_entity,
    can_atomically_merge_entity,
    generate_child_entity_trees,
    get_all_entity_trees_of_cls,
    get_all_person_external_ids,
    merge_flat_fields,
    nonnull_fields_entity_match,
    remove_child_from_entity,
)
from recidiviz.persistence.errors import EntityMatchingError
from recidiviz.tests.persistence.entity.entity_utils_test import (
    HAS_MEANINGFUL_DATA_ENTITIES,
    PLACEHOLDER_ENTITY_EXAMPLES,
    REFERENCE_ENTITY_EXAMPLES,
)
from recidiviz.tests.persistence.entity_matching.state.base_state_entity_matcher_test_classes import (
    BaseStateMatchingUtilsTest,
)

_EXTERNAL_ID = "EXTERNAL_ID-1"
_EXTERNAL_ID_2 = "EXTERNAL_ID-2"
_EXTERNAL_ID_3 = "EXTERNAL_ID-3"
_ID = 1
_ID_2 = 2
_ID_3 = 3
_COUNTY_CODE = "COUNTY"
_COUNTY_CODE_ANOTHER = "COUNTY_ANOTHER"
_STATE_CODE = "US_XX"
_STATE_CODE_ANOTHER = "US_XY"
_ID_TYPE = "ID_TYPE"
_ID_TYPE_ANOTHER = "ID_TYPE_ANOTHER"


# pylint: disable=protected-access
class TestStateMatchingUtils(BaseStateMatchingUtilsTest):
    """Tests for state entity matching utils"""

    def test_isMatch_statePerson(self) -> None:
        external_id = schema.StatePersonExternalId(
            state_code=_STATE_CODE, external_id=_EXTERNAL_ID
        )
        external_id_same = schema.StatePersonExternalId(
            state_code=_STATE_CODE, external_id=_EXTERNAL_ID
        )
        external_id_different = schema.StatePersonExternalId(
            state_code=_STATE_CODE, external_id=_EXTERNAL_ID_2
        )

        person = schema.StatePerson(full_name="name", external_ids=[external_id])
        person_another = schema.StatePerson(
            full_name="name_2", external_ids=[external_id_same]
        )

        self.assertTrue(
            _is_match(
                ingested_entity=person,
                db_entity=person_another,
                field_index=self.field_index,
            )
        )
        person_another.external_ids = [external_id_different]
        self.assertFalse(
            _is_match(
                ingested_entity=person,
                db_entity=person_another,
                field_index=self.field_index,
            )
        )

    def test_isMatch_statePersonExternalId_type(self) -> None:
        external_id = schema.StatePersonExternalId(
            person_external_id_id=_ID,
            state_code=_STATE_CODE,
            id_type=_ID_TYPE,
            external_id=_EXTERNAL_ID,
        )
        external_id_different = schema.StatePersonExternalId(
            person_external_id_id=None,
            state_code=_STATE_CODE,
            id_type=_ID_TYPE,
            external_id=_EXTERNAL_ID,
        )
        self.assertTrue(
            _is_match(
                ingested_entity=external_id,
                db_entity=external_id_different,
                field_index=self.field_index,
            )
        )

        external_id.id_type = _ID_TYPE_ANOTHER
        self.assertFalse(
            _is_match(
                ingested_entity=external_id,
                db_entity=external_id_different,
                field_index=self.field_index,
            )
        )

    def test_isMatch_statePersonExternalId_externalId(self) -> None:
        external_id = schema.StatePersonExternalId(
            person_external_id_id=_ID,
            state_code=_STATE_CODE,
            id_type=_ID_TYPE,
            external_id=_EXTERNAL_ID,
        )
        external_id_different = schema.StatePersonExternalId(
            person_external_id_id=None,
            state_code=_STATE_CODE,
            id_type=_ID_TYPE,
            external_id=_EXTERNAL_ID,
        )
        self.assertTrue(
            _is_match(
                ingested_entity=external_id,
                db_entity=external_id_different,
                field_index=self.field_index,
            )
        )

        external_id.external_id = _EXTERNAL_ID_2
        self.assertFalse(
            _is_match(
                ingested_entity=external_id,
                db_entity=external_id_different,
                field_index=self.field_index,
            )
        )

    def test_isMatch_statePersonExternalId_stateCode(self) -> None:
        external_id = schema.StatePersonExternalId(
            person_external_id_id=_ID,
            state_code=_STATE_CODE,
            id_type=_ID_TYPE,
            external_id=_EXTERNAL_ID,
        )
        external_id_different = schema.StatePersonExternalId(
            person_external_id_id=None,
            state_code=_STATE_CODE,
            id_type=_ID_TYPE,
            external_id=_EXTERNAL_ID,
        )
        self.assertTrue(
            _is_match(
                ingested_entity=external_id,
                db_entity=external_id_different,
                field_index=self.field_index,
            )
        )

        external_id.state_code = _STATE_CODE_ANOTHER
        self.assertFalse(
            _is_match(
                ingested_entity=external_id,
                db_entity=external_id_different,
                field_index=self.field_index,
            )
        )

    def test_isMatch_statePersonAlias(self) -> None:
        alias = schema.StatePersonAlias(state_code=_STATE_CODE, full_name="full_name")
        alias_another = schema.StatePersonAlias(
            state_code=_STATE_CODE, full_name="full_name"
        )
        self.assertTrue(
            _is_match(
                ingested_entity=alias,
                db_entity=alias_another,
                field_index=self.field_index,
            )
        )
        alias_another.state_code = _STATE_CODE_ANOTHER
        self.assertFalse(
            _is_match(
                ingested_entity=alias,
                db_entity=alias_another,
                field_index=self.field_index,
            )
        )

    def test_isMatch_defaultCompareExternalId(self) -> None:
        charge = schema.StateCharge(external_id=_EXTERNAL_ID, description="description")
        charge_another = schema.StateCharge(
            external_id=_EXTERNAL_ID, description="description_another"
        )
        self.assertTrue(
            _is_match(
                ingested_entity=charge,
                db_entity=charge_another,
                field_index=self.field_index,
            )
        )
        charge.external_id = _EXTERNAL_ID_2
        self.assertFalse(
            _is_match(
                ingested_entity=charge,
                db_entity=charge_another,
                field_index=self.field_index,
            )
        )

    def test_isMatch_defaultCompareNoExternalIds(self) -> None:
        charge = schema.StateCharge()
        charge_another = schema.StateCharge()
        self.assertTrue(
            _is_match(
                ingested_entity=charge,
                db_entity=charge_another,
                field_index=self.field_index,
            )
        )
        charge.description = "description"
        self.assertFalse(
            _is_match(
                ingested_entity=charge,
                db_entity=charge_another,
                field_index=self.field_index,
            )
        )

    def test_mergeFlatFields_twoDbEntities(self) -> None:
        to_entity = schema.StateIncarcerationSentence(
            state_code=_STATE_CODE,
            incarceration_sentence_id=_ID,
            county_code="county_code",
            status=StateSentenceStatus.SERVING,
        )
        from_entity = schema.StateIncarcerationSentence(
            state_code=_STATE_CODE,
            incarceration_sentence_id=_ID_2,
            county_code="county_code-updated",
            max_length_days=10,
            status=StateSentenceStatus.PRESENT_WITHOUT_INFO.value,
        )

        expected_entity = schema.StateIncarcerationSentence(
            state_code=_STATE_CODE,
            incarceration_sentence_id=_ID,
            county_code="county_code-updated",
            max_length_days=10,
            status=StateSentenceStatus.SERVING.value,
        )

        merged_entity = merge_flat_fields(
            new_entity=from_entity, old_entity=to_entity, field_index=self.field_index
        )
        self.assert_schema_objects_equal(expected_entity, merged_entity)

    def test_mergeFlatFields(self) -> None:
        ing_entity = schema.StateIncarcerationSentence(
            state_code=_STATE_CODE,
            county_code="county_code-updated",
            max_length_days=10,
            status=StateSentenceStatus.PRESENT_WITHOUT_INFO.value,
        )
        db_entity = schema.StateIncarcerationSentence(
            state_code=_STATE_CODE,
            incarceration_sentence_id=_ID,
            county_code="county_code",
            status=StateSentenceStatus.SERVING,
        )
        expected_entity = schema.StateIncarcerationSentence(
            state_code=_STATE_CODE,
            incarceration_sentence_id=_ID,
            county_code="county_code-updated",
            max_length_days=10,
            status=StateSentenceStatus.SERVING.value,
        )

        merged_entity = merge_flat_fields(
            new_entity=ing_entity, old_entity=db_entity, field_index=self.field_index
        )
        self.assert_schema_objects_equal(expected_entity, merged_entity)

    @staticmethod
    def convert_to_entity(db_entity: DatabaseEntity) -> Entity:
        return StateSchemaToEntityConverter().convert_all([db_entity])[0]

    def test_mergeFlatFields_DoNotOverwriteWithPlaceholder(self) -> None:
        field_index = CoreEntityFieldIndex()
        for db_entity_cls in get_non_history_state_database_entities():
            for old in HAS_MEANINGFUL_DATA_ENTITIES[db_entity_cls]:
                for new in PLACEHOLDER_ENTITY_EXAMPLES[db_entity_cls]:
                    if old.get_external_id() != new.get_external_id():
                        continue
                    # Expect unchanged
                    expected = self.convert_to_entity(old)
                    updated = merge_flat_fields(
                        new_entity=new,
                        old_entity=old,
                        field_index=field_index,
                    )
                    self.assertEqual(expected, self.convert_to_entity(updated))

    def test_mergeFlatFields_DoNotOverwriteWithReferenceItems(self) -> None:
        field_index = CoreEntityFieldIndex()
        for db_entity_cls in get_non_history_state_database_entities():
            for old in HAS_MEANINGFUL_DATA_ENTITIES[db_entity_cls]:
                for new in REFERENCE_ENTITY_EXAMPLES[db_entity_cls]:
                    if old.get_external_id() != new.get_external_id():
                        continue
                    # Expect unchanged
                    expected = self.convert_to_entity(old)
                    updated = merge_flat_fields(
                        new_entity=new,
                        old_entity=old,
                        field_index=field_index,
                    )
                    self.assertEqual(expected, self.convert_to_entity(updated))

    def test_mergeFlatFields_AtomicEnums(self) -> None:
        ing_entity = schema.StatePerson(
            state_code=_STATE_CODE,
            gender=Gender.EXTERNAL_UNKNOWN,
        )
        db_entity = schema.StatePerson(
            state_code=_STATE_CODE,
            gender_raw_text="XXXX",
            birthdate=datetime.date(1992, 1, 1),
        )
        expected_entity = schema.StatePerson(
            state_code=_STATE_CODE,
            gender=Gender.EXTERNAL_UNKNOWN,
            # Old gender_raw_text is cleared
            birthdate=datetime.date(1992, 1, 1),
        )

        merged_entity = merge_flat_fields(
            new_entity=ing_entity, old_entity=db_entity, field_index=self.field_index
        )
        self.assert_schema_objects_equal(expected_entity, merged_entity)

    def test_mergeFlatFields_AtomicEnums2(self) -> None:
        ing_entity = schema.StatePerson(
            state_code=_STATE_CODE,
            gender_raw_text="XXXX",
        )
        db_entity = schema.StatePerson(
            state_code=_STATE_CODE,
            gender=Gender.EXTERNAL_UNKNOWN,
            birthdate=datetime.date(1992, 1, 1),
        )
        expected_entity = schema.StatePerson(
            state_code=_STATE_CODE,
            # Old gender is cleared
            gender_raw_text="XXXX",
            birthdate=datetime.date(1992, 1, 1),
        )

        merged_entity = merge_flat_fields(
            new_entity=ing_entity, old_entity=db_entity, field_index=self.field_index
        )
        self.assert_schema_objects_equal(expected_entity, merged_entity)

    def test_generateChildEntitiesWithAncestorChain(self) -> None:
        charge = schema.StateCharge(state_code=_STATE_CODE, charge_id=_ID)

        charge_2 = schema.StateCharge(state_code=_STATE_CODE, charge_id=_ID_2)

        sentence = schema.StateIncarcerationSentence(
            status=StateSentenceStatus.PRESENT_WITHOUT_INFO,
            state_code=_STATE_CODE,
            charges=[charge, charge_2],
        )

        person = schema.StatePerson(
            state_code=_STATE_CODE,
            person_id=_ID,
            incarceration_sentences=[sentence],
        )

        sentence_tree = EntityTree(entity=sentence, ancestor_chain=[person])

        expected_child_trees = [
            EntityTree(entity=charge, ancestor_chain=[person, sentence]),
            EntityTree(entity=charge_2, ancestor_chain=[person, sentence]),
        ]

        self.assertEqual(
            expected_child_trees,
            generate_child_entity_trees("charges", [sentence_tree]),
        )

    def test_addChildToEntity(self) -> None:
        charge = schema.StateCharge(state_code=_STATE_CODE, charge_id=_ID)
        sentence = schema.StateIncarcerationSentence(
            status=StateSentenceStatus.PRESENT_WITHOUT_INFO,
            state_code=_STATE_CODE,
            charges=[],
        )

        add_child_to_entity(
            entity=sentence, child_field_name="charges", child_to_add=charge
        )
        self.assertEqual(sentence.charges, [charge])

    def test_addChildToEntity_singular(self) -> None:
        charge = schema.StateCharge(
            state_code=_STATE_CODE,
            status=StateChargeStatus.PRESENT_WITHOUT_INFO,
            charge_id=_ID,
        )
        court_case = schema.StateCourtCase(state_code=_STATE_CODE, court_case_id=_ID)

        add_child_to_entity(
            entity=charge, child_field_name="court_case", child_to_add=court_case
        )
        self.assertEqual(charge.court_case, court_case)

    def test_removeChildFromEntity(self) -> None:
        charge = schema.StateCharge(state_code=_STATE_CODE, charge_id=_ID)
        charge_another = schema.StateCharge(state_code=_STATE_CODE, charge_id=_ID_2)
        sentence = schema.StateIncarcerationSentence(
            status=StateSentenceStatus.PRESENT_WITHOUT_INFO,
            state_code=_STATE_CODE,
            charges=[charge, charge_another],
        )

        remove_child_from_entity(
            entity=sentence,
            child_field_name="charges",
            child_to_remove=charge_another,
        )
        self.assertEqual(sentence.charges, [charge])

    def test_getAllEntityTreesOfCls(self) -> None:
        incarceration_sentence = schema.StateIncarcerationSentence(
            incarceration_sentence_id=_ID
        )
        incarceration_sentence_2 = schema.StateIncarcerationSentence(
            incarceration_sentence_id=_ID_2
        )
        person = schema.StatePerson(
            person_id=_ID,
            incarceration_sentences=[incarceration_sentence, incarceration_sentence_2],
        )

        self.assertEqual(
            [
                EntityTree(entity=incarceration_sentence, ancestor_chain=[person]),
                EntityTree(entity=incarceration_sentence_2, ancestor_chain=[person]),
            ],
            get_all_entity_trees_of_cls(
                [person],
                schema.StateIncarcerationSentence,
                field_index=self.field_index,
            ),
        )
        self.assertEqual(
            [EntityTree(entity=person, ancestor_chain=[])],
            get_all_entity_trees_of_cls(
                [person], schema.StatePerson, field_index=self.field_index
            ),
        )

    def test_getAllPersonExternalIds(self) -> None:
        supervision_sentence = schema.StateSupervisionSentence(external_id=_EXTERNAL_ID)
        supervision_sentence_2 = schema.StateSupervisionSentence(
            external_id=_EXTERNAL_ID_2
        )
        supervision_sentence_3 = schema.StateSupervisionSentence(
            external_id=_EXTERNAL_ID_3
        )
        external_id = schema.StatePersonExternalId(external_id=_EXTERNAL_ID)
        person = schema.StatePerson(
            external_ids=[external_id],
            supervision_sentences=[
                supervision_sentence,
                supervision_sentence_2,
                supervision_sentence_3,
            ],
        )

        self.assertCountEqual([_EXTERNAL_ID], get_all_person_external_ids([person]))

    def test_getAllPersonExternalIds_emptyExternalId_raises(self) -> None:
        incarceration_sentence = schema.StateIncarcerationSentence(
            external_id=_EXTERNAL_ID
        )
        person = schema.StatePerson(
            external_ids=[],
            incarceration_sentences=[incarceration_sentence],
        )

        with self.assertRaises(EntityMatchingError):
            get_all_person_external_ids([person])

    def test_getAllPersonExternalIds_emptyPersonExternalId_raises(self) -> None:
        person = schema.StatePerson()
        with self.assertRaises(EntityMatchingError):
            get_all_person_external_ids([person])

    def test_completeEnumSet_admittedForCommitmentFromSupervision(self) -> None:
        period = schema.StateIncarcerationPeriod()
        for admission_reason in StateIncarcerationPeriodAdmissionReason:
            period.admission_reason = admission_reason.value
            is_commitment_from_supervision(
                StateIncarcerationPeriodAdmissionReason.parse_from_canonical_string(
                    period.admission_reason,
                ),
                allow_ingest_only_enum_values=True,
            )

        is_commitment_from_supervision(
            StateIncarcerationPeriodAdmissionReason.parse_from_canonical_string(None)
        )

    def test_nonnullFieldsEntityMatch_placeholder(self) -> None:
        charge = schema.StateCharge(
            state_code=_STATE_CODE, status=StateChargeStatus.PRESENT_WITHOUT_INFO
        )
        charge_another = schema.StateCharge(
            state_code=_STATE_CODE, status=StateChargeStatus.PRESENT_WITHOUT_INFO
        )
        self.assertFalse(
            nonnull_fields_entity_match(
                ingested_entity=EntityTree(entity=charge, ancestor_chain=[]),
                db_entity=EntityTree(entity=charge_another, ancestor_chain=[]),
                field_index=self.field_index,
            )
        )

    def test_nonnullFieldsEntityMatch_externalIdCompare(self) -> None:
        charge = schema.StateCharge(
            state_code=_STATE_CODE,
            status=StateChargeStatus.PRESENT_WITHOUT_INFO,
            external_id=_EXTERNAL_ID,
        )
        charge_another = schema.StateCharge(
            state_code=_STATE_CODE, status=StateChargeStatus.PRESENT_WITHOUT_INFO
        )
        self.assertFalse(
            nonnull_fields_entity_match(
                ingested_entity=EntityTree(entity=charge, ancestor_chain=[]),
                db_entity=EntityTree(entity=charge_another, ancestor_chain=[]),
                field_index=self.field_index,
            )
        )
        charge_another.external_id = _EXTERNAL_ID
        self.assertTrue(
            nonnull_fields_entity_match(
                ingested_entity=EntityTree(entity=charge, ancestor_chain=[]),
                db_entity=EntityTree(entity=charge_another, ancestor_chain=[]),
                field_index=self.field_index,
            )
        )

    def test_nonnullFieldsEntityMatch_flatFieldsCompare(self) -> None:
        charge = schema.StateCharge(
            state_code=_STATE_CODE,
            ncic_code="1234",
            county_code=_COUNTY_CODE,
            status=StateChargeStatus.PRESENT_WITHOUT_INFO,
        )
        charge_another = schema.StateCharge(
            state_code=_STATE_CODE,
            ncic_code="1234",
            status=StateChargeStatus.PRESENT_WITHOUT_INFO,
        )

        # If one of the entities is merely missing a field, we still consider it a match
        self.assertTrue(
            nonnull_fields_entity_match(
                ingested_entity=EntityTree(entity=charge, ancestor_chain=[]),
                db_entity=EntityTree(entity=charge_another, ancestor_chain=[]),
                field_index=self.field_index,
            )
        )
        charge_another.county_code = _COUNTY_CODE_ANOTHER

        # If one of the entities has a different value, then it is not a match
        self.assertFalse(
            nonnull_fields_entity_match(
                ingested_entity=EntityTree(entity=charge, ancestor_chain=[]),
                db_entity=EntityTree(entity=charge_another, ancestor_chain=[]),
                field_index=self.field_index,
            )
        )
        charge_another.county_code = _COUNTY_CODE

        # All fields the same - this is a match
        self.assertTrue(
            nonnull_fields_entity_match(
                ingested_entity=EntityTree(entity=charge, ancestor_chain=[]),
                db_entity=EntityTree(entity=charge_another, ancestor_chain=[]),
                field_index=self.field_index,
            )
        )

    def test_isPlaceholder(self) -> None:
        entity = schema.StateIncarcerationSentence(
            status=StateSentenceStatus.PRESENT_WITHOUT_INFO.value,
            state_code=_STATE_CODE,
            charges=[schema.StateCharge()],
            person=schema.StatePerson(),
        )
        self.assertTrue(is_placeholder(entity, field_index=self.field_index))
        entity.county_code = "county_code"
        self.assertFalse(is_placeholder(entity, field_index=self.field_index))

    def test_isPlaceholder_personWithExternalId(self) -> None:
        incarceration_sentence = schema.StateIncarcerationSentence(
            state_code=_STATE_CODE, status=StateSentenceStatus.PRESENT_WITHOUT_INFO
        )
        person = schema.StatePerson(
            state_code=_STATE_CODE, incarceration_sentences=[incarceration_sentence]
        )
        self.assertTrue(is_placeholder(person, field_index=self.field_index))
        person.external_ids.append(
            schema.StatePersonExternalId(
                state_code=_STATE_CODE, external_id=_EXTERNAL_ID, id_type=_ID_TYPE
            )
        )
        self.assertFalse(is_placeholder(person, field_index=self.field_index))

    def test_isPlaceholder_defaultEnumValue(self) -> None:
        entity = schema.StateIncarcerationSentence(
            incarceration_type=StateIncarcerationType.STATE_PRISON.value
        )
        self.assertTrue(is_placeholder(entity, field_index=self.field_index))

        entity.incarceration_type_raw_text = "PRISON"
        self.assertFalse(is_placeholder(entity, field_index=self.field_index))

    def test_isAtomicallyMergedEntity(self) -> None:
        field_index = CoreEntityFieldIndex()
        for db_entity_cls in get_non_history_state_database_entities():
            for entity in PLACEHOLDER_ENTITY_EXAMPLES[db_entity_cls]:
                self.assertFalse(can_atomically_merge_entity(entity, field_index))
            for entity in REFERENCE_ENTITY_EXAMPLES[db_entity_cls]:
                self.assertFalse(can_atomically_merge_entity(entity, field_index))
            for entity in HAS_MEANINGFUL_DATA_ENTITIES[db_entity_cls]:

                if db_entity_cls in {
                    schema.StateAgent,
                    schema.StateEarlyDischarge,
                    schema.StateIncarcerationSentence,
                    schema.StatePerson,
                }:
                    self.assertFalse(can_atomically_merge_entity(entity, field_index))
                else:
                    self.assertTrue(can_atomically_merge_entity(entity, field_index))
