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

from recidiviz.common.constants.state.state_sentence import StateSentenceStatus
from recidiviz.persistence.database.schema.state import schema
from recidiviz.persistence.database.schema_entity_converter import (
    schema_entity_converter as converter,
)
from recidiviz.persistence.entity.county.entities import Booking, Charge, Hold, Person
from recidiviz.persistence.entity.state import entities

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
                state_code="US_XX"
            ).get_entity_name(),
        )
        self.assertEqual(
            "state_court_case",
            entities.StateCourtCase.new_with_defaults(
                state_code="US_XX"
            ).get_entity_name(),
        )
        self.assertEqual(
            "state_supervision_violation_response",
            entities.StateSupervisionViolationResponse.new_with_defaults(
                state_code="US_XX"
            ).get_entity_name(),
        )

    def test_get_entity_name_county_entity_sample(self) -> None:
        self.assertEqual("person", Person.new_with_defaults().get_entity_name())
        self.assertEqual("booking", Booking.new_with_defaults().get_entity_name())
        self.assertEqual("hold", Hold.new_with_defaults().get_entity_name())

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
                person_race_id=456, state_code="US_XX"
            ).get_id(),
        )
        self.assertEqual(
            789,
            entities.StateCourtCase.new_with_defaults(
                court_case_id=789, state_code="US_XX"
            ).get_id(),
        )
        self.assertEqual(
            901,
            entities.StateSupervisionViolationResponse.new_with_defaults(
                supervision_violation_response_id=901, state_code="US_XX"
            ).get_id(),
        )
        self.assertIsNone(
            entities.StatePerson.new_with_defaults(state_code="US_XX").get_id()
        )

    def test_get_id_county_entity_sample(self) -> None:
        self.assertEqual(123, Person.new_with_defaults(person_id=123).get_id())
        self.assertEqual(456, Booking.new_with_defaults(booking_id=456).get_id())
        self.assertEqual(789, Hold.new_with_defaults(hold_id=789).get_id())
        self.assertIsNone(Charge.new_with_defaults().get_id())

    def test_get_class_id_name_state_entity_sample(self) -> None:
        self.assertEqual("person_id", entities.StatePerson.get_class_id_name())
        self.assertEqual(
            "supervision_contact_id",
            entities.StateSupervisionContact.get_class_id_name(),
        )
        self.assertEqual("court_case_id", entities.StateCourtCase.get_class_id_name())
        self.assertEqual(
            "supervision_violation_response_id",
            entities.StateSupervisionViolationResponse.get_class_id_name(),
        )

    def test_class_id_name_county_entity_sample(self) -> None:
        self.assertEqual("person_id", Person.get_class_id_name())
        self.assertEqual("booking_id", Booking.get_class_id_name())
        self.assertEqual("hold_id", Hold.get_class_id_name())

    def test_getField(self) -> None:
        entity = entities.StatePerson.new_with_defaults(
            state_code="US_XX",
            current_address=None,
        )
        db_entity = converter.convert_entity_to_schema_object(entity)

        self.assertEqual("US_XX", entity.get_field("state_code"))
        self.assertEqual("US_XX", db_entity.get_field("state_code"))

        with self.assertRaises(ValueError):
            entity.get_field("bad_current_address")

        with self.assertRaises(ValueError):
            db_entity.get_field("bad_current_address")

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
            state_code="US_XX", status=entities.ChargeStatus.PRESENT_WITHOUT_INFO
        )
        db_entity = converter.convert_entity_to_schema_object(entity)
        if not isinstance(db_entity, schema.StateCharge):
            self.fail(f"Unexpected type for db_entity: {[db_entity]}.")

        self.assertTrue(entity.is_default_enum("status"))
        entity.status = entities.ChargeStatus.CONVICTED
        self.assertFalse(entity.is_default_enum("status"))

        self.assertTrue(db_entity.is_default_enum("status"))
        db_entity.status = entities.ChargeStatus.CONVICTED.value
        self.assertFalse(db_entity.is_default_enum("status"))

    def test_isDefaultEnum_incarceration_type(self) -> None:
        entity = entities.StateIncarcerationSentence.new_with_defaults(
            state_code="US_XX",
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
