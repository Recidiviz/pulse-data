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
"""Tests for serialization methods regarding entities."""
import datetime
import unittest

from recidiviz.common.constants.state.state_assessment import StateAssessmentClass
from recidiviz.common.constants.state.state_charge import (
    StateChargeStatus,
    StateChargeV2Status,
)
from recidiviz.common.constants.state.state_person import StateGender
from recidiviz.common.constants.state.state_sentence import StateSentenceStatus
from recidiviz.persistence.entity.serialization import (
    json_serializable_dict,
    serialize_entity_into_json,
)
from recidiviz.persistence.entity.state import entities, normalized_entities
from recidiviz.persistence.entity.state.entities import (
    StateAssessment,
    StateChargeV2,
    StatePerson,
    StateSentence,
)
from recidiviz.persistence.entity.state.normalized_entities import (
    NormalizedStateAssessment,
    NormalizedStateCharge,
    NormalizedStateIncarcerationSentence,
    NormalizedStatePerson,
)
from recidiviz.pipelines.metrics.utils.metric_utils import (
    json_serializable_list_value_handler,
)


class TestJsonSerializableDict(unittest.TestCase):
    """Tests using the json_serializable_dict function with the
    json_serializable_list_value_handler built for handling lists in metric values."""

    def test_json_serializable_dict(self) -> None:
        metric_key = {
            "gender": StateGender.MALE,
            "year": 1999,
            "month": 3,
            "state_code": "CA",
        }

        expected_output = {
            "gender": "MALE",
            "year": 1999,
            "month": 3,
            "state_code": "CA",
        }

        updated_metric_key = json_serializable_dict(
            metric_key, list_serializer=json_serializable_list_value_handler
        )

        self.assertEqual(expected_output, updated_metric_key)

    def test_json_serializable_dict_date_handling(self) -> None:
        metric_key = {
            "date_field": datetime.date(2000, 1, 2),
            "datetime_field": datetime.datetime(2000, 1, 2, 3, 4, 5),
            "datetime_field_midnight": datetime.datetime(2000, 1, 2, 0, 0, 0, 0),
            "datetime_field_with_millis": datetime.datetime(2000, 1, 2, 3, 4, 5, 6),
            "null_date_field": None,
            "null_datetime_field": None,
        }

        expected_output = {
            "date_field": "2000-01-02",
            "datetime_field": "2000-01-02T03:04:05",
            "datetime_field_midnight": "2000-01-02T00:00:00",
            "datetime_field_with_millis": "2000-01-02T03:04:05.000006",
            "null_date_field": None,
            "null_datetime_field": None,
        }

        updated_metric_key = json_serializable_dict(
            metric_key, list_serializer=json_serializable_list_value_handler
        )

        self.assertEqual(expected_output, updated_metric_key)

    def test_json_serializable_dict_ViolationTypeFrequencyCounter(self) -> None:
        metric_key = {
            "gender": StateGender.MALE,
            "year": 1999,
            "month": 3,
            "state_code": "CA",
            "violation_type_frequency_counter": [
                ["TECHNICAL"],
                ["ASC", "EMP", "TECHNICAL"],
            ],
        }

        expected_output = {
            "gender": "MALE",
            "year": 1999,
            "month": 3,
            "state_code": "CA",
            "violation_type_frequency_counter": "[ASC, EMP, TECHNICAL],[TECHNICAL]",
        }

        updated_metric_key = json_serializable_dict(
            metric_key, list_serializer=json_serializable_list_value_handler
        )

        self.assertEqual(expected_output, updated_metric_key)

    def test_json_serializable_dict_InvalidList(self) -> None:
        metric_key = {"invalid_list_key": ["list", "values"]}

        with self.assertRaisesRegex(
            ValueError, "^Unexpected list in metric_key for key: invalid_list_key$"
        ):
            json_serializable_dict(
                metric_key, list_serializer=json_serializable_list_value_handler
            )

    def test_serialize_entity_into_json(self) -> None:
        person = StatePerson(
            person_id=123, state_code="US_XX", gender=StateGender.FEMALE
        )

        assessment = StateAssessment(
            assessment_id=456,
            state_code="US_XX",
            external_id="EXTERNAL_ID",
            assessment_class=StateAssessmentClass.RISK,
            person=person,
        )
        person.assessments = [assessment]

        charge = StateChargeV2(
            charge_v2_id=789,
            state_code="US_XX",
            external_id="EXTERNAL_ID",
            status=StateChargeV2Status.PENDING,
            person=person,
        )

        sentence = StateSentence(
            sentence_id=1011,
            state_code="US_XX",
            external_id="EXTERNAL_ID",
            person=person,
            charges=[charge],
        )
        person.sentences = [sentence]

        # Root entity
        self.assertEqual(
            {
                "birthdate": None,
                "current_address": None,
                "current_email_address": None,
                "current_phone_number": None,
                "full_name": None,
                "gender": "FEMALE",
                "gender_raw_text": None,
                "person_id": 123,
                "residency_status": None,
                "residency_status_raw_text": None,
                "state_code": "US_XX",
            },
            serialize_entity_into_json(person, entities),
        )

        # Simple entity with one-to-many relationship with root
        self.assertEqual(
            {
                "assessment_class": "RISK",
                "assessment_class_raw_text": None,
                "assessment_date": None,
                "assessment_id": 456,
                "assessment_level": None,
                "assessment_level_raw_text": None,
                "assessment_metadata": None,
                "assessment_score": None,
                "assessment_type": None,
                "assessment_type_raw_text": None,
                "conducting_staff_external_id": None,
                "conducting_staff_external_id_type": None,
                "external_id": "EXTERNAL_ID",
                "person_id": 123,
                "state_code": "US_XX",
            },
            serialize_entity_into_json(assessment, entities),
        )

        # Entity with many-to-many relationship with child
        self.assertEqual(
            {
                "conditions": None,
                "county_code": None,
                "current_state_provided_start_date": None,
                "external_id": "EXTERNAL_ID",
                "imposed_date": None,
                "initial_time_served_days": None,
                "is_capital_punishment": None,
                "is_life": None,
                "parent_sentence_external_id_array": None,
                "parole_possible": None,
                "person_id": 123,
                "sentence_group_external_id": None,
                "sentence_id": 1011,
                "sentence_metadata": None,
                "sentence_type": None,
                "sentence_type_raw_text": None,
                "sentencing_authority": None,
                "sentencing_authority_raw_text": None,
                "state_code": "US_XX",
            },
            serialize_entity_into_json(sentence, entities),
        )

        # Entity with indirect connection to root entity
        self.assertEqual(
            {
                "attempted": None,
                "charge_notes": None,
                "charge_v2_id": 789,
                "charging_entity": None,
                "classification_subtype": None,
                "classification_type": None,
                "classification_type_raw_text": None,
                "counts": None,
                "county_code": None,
                "date_charged": None,
                "description": None,
                "external_id": "EXTERNAL_ID",
                "is_controlling": None,
                "is_drug": None,
                "is_sex_offense": None,
                "is_violent": None,
                "judge_external_id": None,
                "judge_full_name": None,
                "judicial_district_code": None,
                "ncic_code": None,
                "offense_date": None,
                "offense_type": None,
                "person_id": 123,
                "state_code": "US_XX",
                "status": "PENDING",
                "status_raw_text": None,
                "statute": None,
            },
            serialize_entity_into_json(charge, entities),
        )

    def test_serialize_entity_into_json_normalized(self) -> None:
        person = NormalizedStatePerson(
            person_id=123, state_code="US_XX", gender=StateGender.FEMALE
        )

        assessment = NormalizedStateAssessment(
            assessment_id=456,
            state_code="US_XX",
            external_id="EXTERNAL_ID",
            assessment_class=StateAssessmentClass.RISK,
            person=person,
            sequence_num=1,
        )
        person.assessments = [assessment]

        charge = NormalizedStateCharge(
            charge_id=789,
            state_code="US_XX",
            external_id="EXTERNAL_ID",
            status=StateChargeStatus.PENDING,
            person=person,
        )

        sentence = NormalizedStateIncarcerationSentence(
            incarceration_sentence_id=1011,
            state_code="US_XX",
            external_id="EXTERNAL_ID",
            person=person,
            status=StateSentenceStatus.PENDING,
            charges=[charge],
        )
        person.incarceration_sentences = [sentence]

        # Root entity
        self.assertEqual(
            {
                "birthdate": None,
                "current_address": None,
                "current_email_address": None,
                "current_phone_number": None,
                "full_name": None,
                "gender": "FEMALE",
                "gender_raw_text": None,
                "person_id": 123,
                "residency_status": None,
                "residency_status_raw_text": None,
                "state_code": "US_XX",
            },
            serialize_entity_into_json(person, normalized_entities),
        )

        # Simple entity with one-to-many relationship with root
        self.assertEqual(
            {
                "assessment_class": "RISK",
                "assessment_class_raw_text": None,
                "assessment_date": None,
                "assessment_id": 456,
                "assessment_level": None,
                "assessment_level_raw_text": None,
                "assessment_metadata": None,
                "assessment_score": None,
                "assessment_score_bucket": None,
                "assessment_type": None,
                "assessment_type_raw_text": None,
                "conducting_staff_external_id": None,
                "conducting_staff_external_id_type": None,
                "conducting_staff_id": None,
                "external_id": "EXTERNAL_ID",
                "person_id": 123,
                "sequence_num": 1,
                "state_code": "US_XX",
            },
            serialize_entity_into_json(assessment, normalized_entities),
        )

        # Entity with many-to-many relationship with child
        self.assertEqual(
            {
                "completion_date": None,
                "conditions": None,
                "county_code": None,
                "date_imposed": None,
                "earned_time_days": None,
                "effective_date": None,
                "external_id": "EXTERNAL_ID",
                "good_time_days": None,
                "incarceration_sentence_id": 1011,
                "incarceration_type": None,
                "incarceration_type_raw_text": None,
                "initial_time_served_days": None,
                "is_capital_punishment": None,
                "is_life": None,
                "max_length_days": None,
                "min_length_days": None,
                "parole_eligibility_date": None,
                "parole_possible": None,
                "person_id": 123,
                "projected_max_release_date": None,
                "projected_min_release_date": None,
                "sentence_metadata": None,
                "state_code": "US_XX",
                "status": "PENDING",
                "status_raw_text": None,
            },
            serialize_entity_into_json(sentence, normalized_entities),
        )

        # Entity with indirect connection to root entity
        self.assertEqual(
            {
                "attempted": None,
                "charge_id": 789,
                "charge_notes": None,
                "charging_entity": None,
                "classification_subtype": None,
                "classification_type": None,
                "classification_type_raw_text": None,
                "counts": None,
                "county_code": None,
                "date_charged": None,
                "description": None,
                "description_external": None,
                "external_id": "EXTERNAL_ID",
                "is_controlling": None,
                "is_drug": None,
                "is_drug_external": None,
                "is_sex_offense": None,
                "is_sex_offense_external": None,
                "is_violent": None,
                "is_violent_external": None,
                "judge_external_id": None,
                "judge_full_name": None,
                "judicial_district_code": None,
                "ncic_category_external": None,
                "ncic_code": None,
                "ncic_code_external": None,
                "offense_date": None,
                "offense_type": None,
                "person_id": 123,
                "state_code": "US_XX",
                "status": "PENDING",
                "status_raw_text": None,
                "statute": None,
            },
            serialize_entity_into_json(charge, normalized_entities),
        )

    def test_serialize_entity_into_json_null_person(self) -> None:
        assessment = StateAssessment(
            assessment_id=456,
            state_code="US_XX",
            external_id="EXTERNAL_ID",
            assessment_class=StateAssessmentClass.RISK,
            person=None,
        )

        self.assertEqual(
            {
                "assessment_class": "RISK",
                "assessment_class_raw_text": None,
                "assessment_date": None,
                "assessment_id": 456,
                "assessment_level": None,
                "assessment_level_raw_text": None,
                "assessment_metadata": None,
                "assessment_score": None,
                "assessment_type": None,
                "assessment_type_raw_text": None,
                "conducting_staff_external_id": None,
                "conducting_staff_external_id_type": None,
                "external_id": "EXTERNAL_ID",
                # person_id field is still added with a null value
                "person_id": None,
                "state_code": "US_XX",
            },
            serialize_entity_into_json(assessment, entities),
        )
