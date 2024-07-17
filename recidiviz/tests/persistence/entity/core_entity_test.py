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
import datetime
import unittest

from recidiviz.common.constants.state.state_person import StateRace
from recidiviz.common.constants.state.state_sentence import StateSentenceStatus
from recidiviz.persistence.database.schema.state import schema
from recidiviz.persistence.database.schema_entity_converter import (
    schema_entity_converter as converter,
)
from recidiviz.persistence.entity.state import entities, normalized_entities
from recidiviz.utils.types import assert_type

_STATE_CODE = "US_ND"


class TestCoreEntity(unittest.TestCase):
    """Tests for CoreEntity functionality."""

    def test_get_entity_name_state_entity_sample(self) -> None:
        self.assertEqual(
            "state_person",
            entities.StatePerson.new_with_defaults(
                state_code="US_XX"
            ).get_entity_name(),
        )
        self.assertEqual(
            "state_person_race",
            entities.StatePersonRace.new_with_defaults(
                state_code="US_XX",
                race=StateRace.WHITE,
            ).get_entity_name(),
        )
        self.assertEqual(
            "state_supervision_violation_response",
            entities.StateSupervisionViolationResponse.new_with_defaults(
                state_code="US_XX",
                external_id="svr1",
            ).get_entity_name(),
        )

    def test_get_id_state_entity_sample(self) -> None:
        self.assertEqual(
            123,
            entities.StatePerson.new_with_defaults(
                person_id=123, state_code="US_XX"
            ).get_id(),
        )
        self.assertEqual(
            456,
            entities.StatePersonRace.new_with_defaults(
                person_race_id=456,
                state_code="US_XX",
                race=StateRace.WHITE,
            ).get_id(),
        )
        self.assertEqual(
            901,
            entities.StateSupervisionViolationResponse.new_with_defaults(
                supervision_violation_response_id=901,
                state_code="US_XX",
                external_id="svr1",
            ).get_id(),
        )
        self.assertIsNone(
            entities.StatePerson.new_with_defaults(state_code="US_XX").get_id()
        )

    def test_get_class_id_name_state_entity_sample(self) -> None:
        self.assertEqual("person_id", entities.StatePerson.get_class_id_name())
        self.assertEqual(
            "supervision_contact_id",
            entities.StateSupervisionContact.get_class_id_name(),
        )
        self.assertEqual(
            "supervision_violation_response_id",
            entities.StateSupervisionViolationResponse.get_class_id_name(),
        )

    def test_get_table_id_state_entity_sample(self) -> None:
        self.assertEqual("state_person", entities.StatePerson.get_table_id())
        self.assertEqual(
            "state_supervision_contact",
            entities.StateSupervisionContact.get_table_id(),
        )
        self.assertEqual(
            "state_supervision_violation_response",
            entities.StateSupervisionViolationResponse.get_table_id(),
        )
        self.assertEqual(
            "state_assessment",
            normalized_entities.NormalizedStateAssessment.get_table_id(),
        )
        self.assertEqual(
            "state_supervision_violation_response",
            normalized_entities.NormalizedStateSupervisionViolationResponse.get_table_id(),
        )

    def test_getField(self) -> None:
        external_id = entities.StatePersonExternalId.new_with_defaults(
            state_code="US_XX",
            external_id="ABC",
            id_type="US_XX_ID_TYPE",
        )
        entity = entities.StatePerson.new_with_defaults(
            state_code="US_XX",
            current_address=None,
            external_ids=[external_id],
        )
        external_id.person = entity
        db_entity = assert_type(
            converter.convert_entity_to_schema_object(entity), schema.StatePerson
        )
        db_external_id = db_entity.external_ids[0]

        self.assertEqual("US_XX", entity.get_field("state_code"))
        self.assertEqual("US_XX", db_entity.get_field("state_code"))

        with self.assertRaises(ValueError):
            entity.get_field("bad_current_address")

        with self.assertRaises(ValueError):
            db_entity.get_field("bad_current_address")

        self.assertEqual([external_id], entity.get_field("external_ids"))
        self.assertEqual([db_external_id], db_entity.get_field("external_ids"))

        self.assertEqual(entity, external_id.get_field("person"))
        self.assertEqual(db_entity, db_external_id.get_field("person"))

    def test_hasField(self) -> None:
        external_id_cls = entities.StatePersonExternalId
        entity_cls = entities.StatePerson
        db_entity_cls = schema.StatePerson
        db_external_id_cls = schema.StatePersonExternalId

        self.assertTrue(entity_cls.has_field("state_code"))
        self.assertTrue(db_entity_cls.has_field("state_code"))

        self.assertFalse(entity_cls.has_field("bad_current_address"))
        self.assertFalse(db_entity_cls.has_field("bad_current_address"))

        self.assertTrue(entity_cls.has_field("external_ids"))
        self.assertTrue(db_entity_cls.has_field("external_ids"))

        self.assertTrue(external_id_cls.has_field("person"))
        self.assertTrue(db_external_id_cls.has_field("person"))

    def test_hasField_instantiated(self) -> None:
        external_id = entities.StatePersonExternalId.new_with_defaults(
            state_code="US_XX",
            external_id="ABC",
            id_type="US_XX_ID_TYPE",
        )
        entity = entities.StatePerson.new_with_defaults(
            state_code="US_XX",
            current_address=None,
            external_ids=[external_id],
        )
        external_id.person = entity
        db_entity = assert_type(
            converter.convert_entity_to_schema_object(entity), schema.StatePerson
        )
        db_external_id = db_entity.external_ids[0]

        self.assertTrue(entity.has_field("state_code"))
        self.assertTrue(db_entity.has_field("state_code"))

        self.assertFalse(entity.has_field("bad_current_address"))
        self.assertFalse(db_entity.has_field("bad_current_address"))

        self.assertTrue(entity.has_field("external_ids"))
        self.assertTrue(db_entity.has_field("external_ids"))

        self.assertTrue(external_id.has_field("person"))
        self.assertTrue(db_external_id.has_field("person"))

    def test_getFieldAsList(self) -> None:
        assessment = entities.StateAssessment.new_with_defaults(
            state_code="US_XX",
            external_id="ex1",
        )
        assessment_2 = entities.StateAssessment.new_with_defaults(
            state_code="US_XX",
            external_id="ex2",
        )
        entity = entities.StatePerson.new_with_defaults(
            state_code="US_XX",
            assessments=[assessment, assessment_2],
        )
        db_entity = converter.convert_entity_to_schema_object(entity)

        self.assertCountEqual(["US_XX"], entity.get_field_as_list("state_code"))
        self.assertCountEqual(
            [assessment, assessment_2], entity.get_field_as_list("assessments")
        )
        self.assertCountEqual([], entity.get_field_as_list("races"))

        self.assertCountEqual(["US_XX"], db_entity.get_field_as_list("state_code"))
        self.assertCountEqual(
            [assessment, assessment_2],
            converter.convert_schema_objects_to_entity(
                db_entity.get_field_as_list("assessments"), populate_back_edges=False
            ),
        )
        self.assertCountEqual([], db_entity.get_field_as_list("races"))

    def test_clearField(self) -> None:
        assessment = entities.StateAssessment.new_with_defaults(
            state_code="US_XX",
            external_id="ex1",
        )
        assessment_2 = entities.StateAssessment.new_with_defaults(
            state_code="US_XX",
            external_id="ex2",
        )
        entity = entities.StatePerson.new_with_defaults(
            state_code="US_XX",
            assessments=[assessment, assessment_2],
        )
        db_entity = converter.convert_entity_to_schema_object(entity)
        if not isinstance(db_entity, schema.StatePerson):
            self.fail(f"Unexpected type for db_entity: {[db_entity]}.")

        entity.clear_field("state_code")
        entity.clear_field("assessments")
        self.assertIsNone(entity.state_code)
        self.assertCountEqual([], entity.assessments)

        db_entity.clear_field("state_code")
        db_entity.clear_field("assessments")
        self.assertIsNone(db_entity.state_code)
        self.assertCountEqual([], db_entity.assessments)

    def test_setField(self) -> None:
        entity = entities.StatePerson.new_with_defaults(
            state_code="US_XX",
        )
        db_entity = converter.convert_entity_to_schema_object(entity)
        if not isinstance(db_entity, schema.StatePerson):
            self.fail(f"Unexpected type for db_entity: {[db_entity]}.")

        entity.set_field("state_code", "US_XX")
        db_entity.set_field("state_code", "US_XX")

        self.assertEqual("US_XX", entity.state_code)
        self.assertEqual("US_XX", db_entity.state_code)

        with self.assertRaises(ValueError):
            entity.set_field("country_code", "us")

        with self.assertRaises(ValueError):
            db_entity.set_field("country_code", "us")

    def test_setFieldFromList(self) -> None:
        entity = entities.StatePerson.new_with_defaults(
            state_code="US_XX",
        )
        assessment = entities.StateAssessment.new_with_defaults(
            state_code="US_XX",
            external_id="ex1",
        )
        assessment_2 = entities.StateAssessment.new_with_defaults(
            state_code="US_XX",
            external_id="ex2",
        )

        db_entity = converter.convert_entity_to_schema_object(entity)
        if not isinstance(db_entity, schema.StatePerson):
            self.fail(f"Unexpected type for db_entity: {[db_entity]}.")

        db_assessment = converter.convert_entity_to_schema_object(assessment)
        db_assessment_2 = converter.convert_entity_to_schema_object(assessment_2)

        entity.set_field_from_list("state_code", ["US_YY"])
        self.assertEqual("US_YY", entity.state_code)

        db_entity.set_field_from_list("state_code", ["US_YY"])
        self.assertEqual("US_YY", db_entity.state_code)

        entity.set_field_from_list("assessments", [assessment, assessment_2])
        self.assertCountEqual([assessment, assessment_2], entity.assessments)

        db_entity.set_field_from_list("assessments", [db_assessment, db_assessment_2])
        self.assertCountEqual(
            [assessment, assessment_2],
            converter.convert_schema_objects_to_entity(
                db_entity.assessments, populate_back_edges=False
            ),
        )

    def test_setFieldFromList_raises(self) -> None:
        entity = entities.StatePerson.new_with_defaults(
            state_code="US_XX",
        )
        db_entity = converter.convert_entity_to_schema_object(entity)

        with self.assertRaises(ValueError):
            entity.set_field_from_list("state_code", ["US_XX", "US_YY"])

        with self.assertRaises(ValueError):
            db_entity.set_field_from_list("state_code", ["US_XX", "US_YY"])

    def test_isDefaultEnum(self) -> None:
        entity = entities.StateCharge.new_with_defaults(
            state_code="US_XX",
            external_id="c1",
            status=entities.StateChargeStatus.PRESENT_WITHOUT_INFO,
        )
        db_entity = converter.convert_entity_to_schema_object(entity)
        if not isinstance(db_entity, schema.StateCharge):
            self.fail(f"Unexpected type for db_entity: {[db_entity]}.")

        self.assertTrue(entity.is_default_enum("status"))
        entity.status = entities.StateChargeStatus.CONVICTED
        self.assertFalse(entity.is_default_enum("status"))

        self.assertTrue(db_entity.is_default_enum("status"))
        db_entity.status = entities.StateChargeStatus.CONVICTED.value
        self.assertFalse(db_entity.is_default_enum("status"))

    def test_isDefaultEnum_incarceration_type(self) -> None:
        entity = entities.StateIncarcerationSentence.new_with_defaults(
            state_code="US_XX",
            external_id="is1",
            incarceration_type=entities.StateIncarcerationType.STATE_PRISON,
            status=StateSentenceStatus.PRESENT_WITHOUT_INFO,
        )
        db_entity = converter.convert_entity_to_schema_object(entity)
        if not isinstance(db_entity, schema.StateIncarcerationSentence):
            self.fail(f"Unexpected type for db_entity: {[db_entity]}.")

        self.assertTrue(
            entity.is_default_enum(
                "incarceration_type", entities.StateIncarcerationType.STATE_PRISON.value
            )
        )

        entity.incarceration_type_raw_text = "PRISON"
        self.assertFalse(
            entity.is_default_enum(
                "incarceration_type", entities.StateIncarcerationType.STATE_PRISON.value
            )
        )

        entity.incarceration_type = entities.StateIncarcerationType.COUNTY_JAIL
        entity.incarceration_type_raw_text = None
        self.assertFalse(
            entity.is_default_enum(
                "incarceration_type", entities.StateIncarcerationType.STATE_PRISON.value
            )
        )

        self.assertTrue(
            db_entity.is_default_enum(
                "incarceration_type", entities.StateIncarcerationType.STATE_PRISON.value
            )
        )

        db_entity.incarceration_type_raw_text = "PRISON"
        self.assertFalse(
            db_entity.is_default_enum(
                "incarceration_type", entities.StateIncarcerationType.STATE_PRISON.value
            )
        )

        db_entity.incarceration_type = entities.StateIncarcerationType.COUNTY_JAIL.value
        db_entity.incarceration_type_raw_text = None
        self.assertFalse(
            entity.is_default_enum(
                "incarceration_type", entities.StateIncarcerationType.STATE_PRISON.value
            )
        )

    def test_limited_pii_repr_has_external_id_entity(self) -> None:
        entity = entities.StateIncarcerationSentence.new_with_defaults(
            incarceration_sentence_id=123,
            state_code="US_XX",
            external_id="is1",
            incarceration_type=entities.StateIncarcerationType.STATE_PRISON,
            status=StateSentenceStatus.PRESENT_WITHOUT_INFO,
        )
        db_entity = converter.convert_entity_to_schema_object(entity)

        expected_repr = "StateIncarcerationSentence(external_id='is1', incarceration_sentence_id=123)"
        self.assertEqual(expected_repr, entity.limited_pii_repr())
        self.assertEqual(expected_repr, db_entity.limited_pii_repr())

    def test_limited_pii_repr_external_id_entity(self) -> None:
        entity = entities.StateStaffExternalId.new_with_defaults(
            staff_external_id_id=123,
            state_code="US_XX",
            external_id="ABC",
            id_type="US_XX_ID_TYPE",
        )
        db_entity = converter.convert_entity_to_schema_object(entity)

        expected_repr = "StateStaffExternalId(external_id='ABC', id_type='US_XX_ID_TYPE', staff_external_id_id=123)"
        self.assertEqual(expected_repr, entity.limited_pii_repr())
        self.assertEqual(expected_repr, db_entity.limited_pii_repr())

    def test_limited_pii_repr_has_multiple_external_ids_entity(self) -> None:
        entity = entities.StatePerson.new_with_defaults(
            person_id=123,
            state_code="US_XX",
            full_name='{"given_names": "FIRST", "middle_names": "M", "name_suffix": "JR.", "surname": "LAST"}',
            birthdate=datetime.date(1985, 4, 13),
            gender=entities.StateGender.MALE,
            gender_raw_text="MALE",
            external_ids=[
                entities.StatePersonExternalId.new_with_defaults(
                    person_external_id_id=234,
                    state_code="US_XX",
                    external_id="ABC",
                    id_type="US_XX_ID_TYPE",
                )
            ],
        )
        db_entity = converter.convert_entity_to_schema_object(entity)

        expected_repr = "StatePerson(person_id=123, external_ids=[StatePersonExternalId(external_id='ABC', id_type='US_XX_ID_TYPE', person_external_id_id=234)])"
        self.assertEqual(expected_repr, entity.limited_pii_repr())
        self.assertEqual(expected_repr, db_entity.limited_pii_repr())

    def test_limited_pii_repr_enum_entity(self) -> None:
        entity = entities.StatePersonRace.new_with_defaults(
            person_race_id=123,
            state_code="US_XX",
            race=entities.StateRace.WHITE,
            race_raw_text="W",
        )
        db_entity = converter.convert_entity_to_schema_object(entity)

        expected_repr = "StatePersonRace(person_race_id=123)"
        self.assertEqual(expected_repr, entity.limited_pii_repr())
        self.assertEqual(expected_repr, db_entity.limited_pii_repr())
