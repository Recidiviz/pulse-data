# Recidiviz - a data platform for criminal justice reform
# Copyright (C) 2022 Recidiviz, Inc.
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
"""Tests the normalized_entity_conversion_utils.py file."""

import datetime
import unittest
from typing import Set

import attr
import mock
from google.cloud import bigquery
from google.cloud.bigquery.schema import SchemaField

from recidiviz.calculator.pipeline.normalization.utils import normalized_entities
from recidiviz.calculator.pipeline.normalization.utils.normalized_entities import (
    NormalizedStateIncarcerationPeriod,
    NormalizedStateSupervisionCaseTypeEntry,
    NormalizedStateSupervisionViolatedConditionEntry,
    NormalizedStateSupervisionViolation,
    NormalizedStateSupervisionViolationResponse,
)
from recidiviz.calculator.pipeline.normalization.utils.normalized_entities_utils import (
    NORMALIZED_ENTITY_CLASSES,
    AdditionalAttributesMap,
    get_shared_additional_attributes_map_for_entities,
)
from recidiviz.calculator.pipeline.normalization.utils.normalized_entity_conversion_utils import (
    bq_schema_for_normalized_state_entity,
    convert_entities_to_normalized_dicts,
    convert_entity_trees_to_normalized_versions,
    fields_unique_to_normalized_class,
)
from recidiviz.common.constants.shared_enums.charge import ChargeStatus
from recidiviz.common.constants.state.state_incarceration_period import (
    StateSpecializedPurposeForIncarceration,
)
from recidiviz.common.constants.state.state_sentence import StateSentenceStatus
from recidiviz.persistence.entity.entity_utils import CoreEntityFieldIndex
from recidiviz.persistence.entity.state import entities
from recidiviz.persistence.entity.state.entities import (
    StateIncarcerationPeriod,
    StateSupervisionViolatedConditionEntry,
    StateSupervisionViolation,
    StateSupervisionViolationResponse,
    StateSupervisionViolationResponseDecisionEntry,
    StateSupervisionViolationTypeEntry,
)
from recidiviz.tests.calculator.pipeline.normalization.utils.normalized_entities_utils_test import (
    TestNormalizedStateCharge,
    TestNormalizedStateCourtCase,
    TestNormalizedStateSupervisionSentence,
    get_normalized_violation_tree,
    get_violation_tree,
)


class TestBQSchemaForNormalizedStateEntity(unittest.TestCase):
    """Tests the bq_schema_for_normalized_state_entity function."""

    def test_bq_schema_for_normalized_state_entity(self) -> None:
        """Test that we can call this function for all NormalizedStateEntity entities
        without crashing"""
        for entity in NORMALIZED_ENTITY_CLASSES:
            _ = bq_schema_for_normalized_state_entity(entity)

    def test_bq_schema_for_normalized_state_entity_test_output(self) -> None:
        schema_for_entity = bq_schema_for_normalized_state_entity(
            NormalizedStateSupervisionViolatedConditionEntry
        )

        expected_schema = [
            SchemaField("state_code", bigquery.enums.SqlTypeNames.STRING.value),
            SchemaField("condition", bigquery.enums.SqlTypeNames.STRING.value),
            SchemaField(
                "supervision_violated_condition_entry_id",
                bigquery.enums.SqlTypeNames.INTEGER.value,
            ),
            SchemaField(
                "supervision_violation_id",
                bigquery.enums.SqlTypeNames.INTEGER.value,
            ),
            SchemaField("person_id", bigquery.enums.SqlTypeNames.INTEGER.value),
        ]

        self.assertEqual(expected_schema, schema_for_entity)

    def test_bq_schema_for_normalized_state_entity_extra_attributes(self) -> None:
        """Tests that fields that exist on a NormalizedStateEntity but not on the
        base entity are included in the schema columns."""
        normalized_ip = normalized_entities.NormalizedStateIncarcerationPeriod
        base_ip = StateIncarcerationPeriod

        normalized_ip_field_names = set(attr.fields_dict(normalized_ip).keys())
        base_ip_field_names = set(attr.fields_dict(base_ip).keys())
        fields_unique_to_norm_ip = normalized_ip_field_names.difference(
            base_ip_field_names
        )

        self.assertNotEqual(set(), fields_unique_to_norm_ip)

        norm_ip_schema = bq_schema_for_normalized_state_entity(normalized_ip)
        schema_cols = [field.name for field in norm_ip_schema]

        for field in fields_unique_to_norm_ip:
            self.assertIn(field, schema_cols)


class TestFieldsUniqueToNormalizedClass(unittest.TestCase):
    """Tests the fields_unique_to_normalized_class function."""

    def test_fields_unique_to_normalized_class(self) -> None:
        entity = NormalizedStateIncarcerationPeriod

        expected_unique_fields = {"sequence_num", "purpose_for_incarceration_subtype"}

        self.assertEqual(
            expected_unique_fields, fields_unique_to_normalized_class(entity)
        )

    def test_fields_unique_to_normalized_class_no_extra_fields(self) -> None:
        entity = NormalizedStateSupervisionCaseTypeEntry

        expected_unique_fields: Set[str] = set()

        self.assertEqual(
            expected_unique_fields, fields_unique_to_normalized_class(entity)
        )


class TestConvertEntityTreesToNormalizedVersions(unittest.TestCase):
    """Tests the convert_entity_trees_to_normalized_versions function."""

    def setUp(self) -> None:
        self.field_index = CoreEntityFieldIndex()

    def test_convert_entity_trees_to_normalized_versions(self) -> None:
        violation_with_tree = get_violation_tree()

        additional_attributes_map: AdditionalAttributesMap = {
            StateSupervisionViolationResponse.__name__: {
                4: {"sequence_num": 0},
                6: {"sequence_num": 1},
            },
            StateSupervisionViolation.__name__: {},
            StateSupervisionViolationTypeEntry.__name__: {},
            StateSupervisionViolatedConditionEntry.__name__: {},
            StateSupervisionViolationResponseDecisionEntry.__name__: {},
        }

        normalized_trees = convert_entity_trees_to_normalized_versions(
            root_entities=[violation_with_tree],
            normalized_entity_class=NormalizedStateSupervisionViolation,
            additional_attributes_map=additional_attributes_map,
            field_index=self.field_index,
        )

        self.assertEqual([get_normalized_violation_tree()], normalized_trees)

    def test_convert_entity_trees_to_normalized_versions_ips(self) -> None:
        ips = [
            StateIncarcerationPeriod.new_with_defaults(
                state_code="US_XX",
                incarceration_period_id=111,
            ),
            StateIncarcerationPeriod.new_with_defaults(
                state_code="US_XX",
                incarceration_period_id=222,
            ),
        ]

        additional_attributes_map = {
            StateIncarcerationPeriod.__name__: {
                111: {"sequence_num": 0, "purpose_for_incarceration_subtype": "XYZ"},
                222: {"sequence_num": 1, "purpose_for_incarceration_subtype": "AAA"},
            }
        }

        normalized_trees = convert_entity_trees_to_normalized_versions(
            root_entities=ips,
            normalized_entity_class=NormalizedStateIncarcerationPeriod,
            additional_attributes_map=additional_attributes_map,
            field_index=self.field_index,
        )

        expected_normalized_ips = [
            NormalizedStateIncarcerationPeriod.new_with_defaults(
                state_code="US_XX",
                incarceration_period_id=111,
                sequence_num=0,
                purpose_for_incarceration_subtype="XYZ",
            ),
            NormalizedStateIncarcerationPeriod.new_with_defaults(
                state_code="US_XX",
                incarceration_period_id=222,
                sequence_num=1,
                purpose_for_incarceration_subtype="AAA",
            ),
        ]

        self.assertEqual(expected_normalized_ips, normalized_trees)

    def test_convert_entity_trees_to_normalized_versions_subtree(self) -> None:
        """Tests that we can normalize a list of entities that are not directly connected
        to the state person.
        """
        violation_with_tree = get_violation_tree()

        normalized_trees = convert_entity_trees_to_normalized_versions(
            root_entities=violation_with_tree.supervision_violation_responses,
            normalized_entity_class=NormalizedStateSupervisionViolationResponse,
            additional_attributes_map={
                StateSupervisionViolationResponse.__name__: {
                    4: {"sequence_num": 0},
                    6: {"sequence_num": 1},
                },
                StateSupervisionViolationTypeEntry.__name__: {},
                StateSupervisionViolatedConditionEntry.__name__: {},
                StateSupervisionViolationResponseDecisionEntry.__name__: {},
            },
            field_index=self.field_index,
        )

        expected_normalized_svrs = (
            get_normalized_violation_tree().supervision_violation_responses
        )
        for svr in expected_normalized_svrs:
            svr.supervision_violation = None
        self.assertEqual(expected_normalized_svrs, normalized_trees)

    @mock.patch(
        "recidiviz.calculator.pipeline.normalization.utils."
        "normalized_entities_utils.NORMALIZED_ENTITY_CLASSES",
        [
            TestNormalizedStateSupervisionSentence,
            TestNormalizedStateCharge,
            TestNormalizedStateCourtCase,
        ],
    )
    def test_convert_entity_trees_to_normalized_versions_invalid_subtree(self) -> None:
        ss = entities.StateSupervisionSentence.new_with_defaults(
            state_code="US_XX",
            status=StateSentenceStatus.PRESENT_WITHOUT_INFO,
            charges=[
                entities.StateCharge.new_with_defaults(
                    state_code="US_XX", status=ChargeStatus.PRESENT_WITHOUT_INFO
                )
            ],
        )

        ss.charges[0].supervision_sentences = [ss]

        with self.assertRaises(ValueError) as e:
            _ = convert_entity_trees_to_normalized_versions(
                root_entities=[ss],
                normalized_entity_class=TestNormalizedStateSupervisionSentence,
                additional_attributes_map={
                    entities.StateSupervisionSentence.__name__: {},
                    entities.StateCharge.__name__: {},
                },
                field_index=self.field_index,
            )

        self.assertEqual(
            "Recursive normalization conversion cannot support reverse fields that "
            "store lists. Found entity of type StateCharge with the field "
            "[supervision_sentences] that stores a list of type "
            "StateSupervisionSentence. Try normalizing this entity tree with the "
            "StateCharge class as the root instead.",
            e.exception.args[0],
        )

    def test_convert_entity_trees_to_normalized_versions_empty_list(self) -> None:
        normalized_trees = convert_entity_trees_to_normalized_versions(
            root_entities=[],
            normalized_entity_class=NormalizedStateIncarcerationPeriod,
            additional_attributes_map={},
            field_index=self.field_index,
        )

        self.assertEqual([], normalized_trees)


class TestConvertEntitiesToNormalizedDicts(unittest.TestCase):
    """Tests the convert_entities_to_normalized_dicts function."""

    def setUp(self) -> None:
        self.field_index = CoreEntityFieldIndex()

    def test_convert_entities_to_normalized_dicts(self) -> None:
        person_id = 123

        entities_to_convert = [
            StateIncarcerationPeriod.new_with_defaults(
                state_code="US_XX",
                incarceration_period_id=111,
                specialized_purpose_for_incarceration=StateSpecializedPurposeForIncarceration.PAROLE_BOARD_HOLD,
                admission_date=datetime.date(2000, 1, 1),
            )
        ]

        additional_attributes_map: AdditionalAttributesMap = {
            StateIncarcerationPeriod.__name__: {
                111: {"sequence_num": 0, "purpose_for_incarceration_subtype": "XYZ"},
            }
        }

        expected_output = [
            (
                StateIncarcerationPeriod.__name__,
                {
                    "admission_date": datetime.date(2000, 1, 1),
                    "admission_reason": None,
                    "admission_reason_raw_text": None,
                    "county_code": None,
                    "custodial_authority": None,
                    "custodial_authority_raw_text": None,
                    "external_id": None,
                    "facility": None,
                    "facility_security_level": None,
                    "facility_security_level_raw_text": None,
                    "housing_unit": None,
                    "incarceration_period_id": 111,
                    "incarceration_type": None,
                    "incarceration_type_raw_text": None,
                    "projected_release_reason": None,
                    "projected_release_reason_raw_text": None,
                    "release_date": None,
                    "release_reason": None,
                    "release_reason_raw_text": None,
                    "specialized_purpose_for_incarceration": "PAROLE_BOARD_HOLD",
                    "specialized_purpose_for_incarceration_raw_text": None,
                    "state_code": "US_XX",
                    "sequence_num": 0,
                    "purpose_for_incarceration_subtype": "XYZ",
                    "person_id": 123,
                },
            )
        ]

        converted_output = convert_entities_to_normalized_dicts(
            person_id=person_id,
            entities=entities_to_convert,
            additional_attributes_map=additional_attributes_map,
            field_index=self.field_index,
        )

        self.assertEqual(expected_output, converted_output)

    def test_convert_entities_to_normalized_dicts_violations(self) -> None:
        person_id = 123

        entities_to_convert = [get_violation_tree()]

        additional_attributes_map: AdditionalAttributesMap = (
            get_shared_additional_attributes_map_for_entities(entities_to_convert)
        )

        expected_output = [
            (
                StateSupervisionViolation.__name__,
                {
                    "external_id": None,
                    "is_sex_offense": None,
                    "is_violent": False,
                    "person_id": 123,
                    "state_code": "US_XX",
                    "supervision_violation_id": 1,
                    "violation_date": datetime.date(2004, 9, 1),
                },
            ),
            (
                StateSupervisionViolationTypeEntry.__name__,
                {
                    "person_id": 123,
                    "state_code": "US_XX",
                    "supervision_violation_id": 1,
                    "supervision_violation_type_entry_id": 2,
                    "violation_type": "TECHNICAL",
                    "violation_type_raw_text": "TECHNICAL",
                },
            ),
            (
                StateSupervisionViolatedConditionEntry.__name__,
                {
                    "condition": "MISSED CURFEW",
                    "person_id": 123,
                    "state_code": "US_XX",
                    "supervision_violated_condition_entry_id": 3,
                    "supervision_violation_id": 1,
                },
            ),
            (
                StateSupervisionViolationResponseDecisionEntry.__name__,
                {
                    "decision": None,
                    "decision_raw_text": "X",
                    "person_id": 123,
                    "state_code": "US_XX",
                    "supervision_violation_response_decision_entry_id": 5,
                    "supervision_violation_response_id": 4,
                },
            ),
            (
                StateSupervisionViolationResponse.__name__,
                {
                    "deciding_body_type": "SUPERVISION_OFFICER",
                    "deciding_body_type_raw_text": None,
                    "external_id": None,
                    "is_draft": None,
                    "person_id": 123,
                    "response_date": datetime.date(2004, 9, 2),
                    "response_subtype": None,
                    "response_type": "CITATION",
                    "response_type_raw_text": None,
                    "state_code": "US_XX",
                    "supervision_violation_id": 1,
                    "supervision_violation_response_id": 4,
                },
            ),
            (
                StateSupervisionViolationResponseDecisionEntry.__name__,
                {
                    "decision": None,
                    "decision_raw_text": "Y",
                    "person_id": 123,
                    "state_code": "US_XX",
                    "supervision_violation_response_decision_entry_id": 7,
                    "supervision_violation_response_id": 6,
                },
            ),
            (
                StateSupervisionViolationResponse.__name__,
                {
                    "deciding_body_type": "SUPERVISION_OFFICER",
                    "deciding_body_type_raw_text": None,
                    "external_id": None,
                    "is_draft": None,
                    "person_id": 123,
                    "response_date": datetime.date(2004, 10, 3),
                    "response_subtype": None,
                    "response_type": "VIOLATION_REPORT",
                    "response_type_raw_text": None,
                    "state_code": "US_XX",
                    "supervision_violation_id": 1,
                    "supervision_violation_response_id": 6,
                },
            ),
        ]

        converted_output = convert_entities_to_normalized_dicts(
            person_id=person_id,
            entities=entities_to_convert,
            additional_attributes_map=additional_attributes_map,
            field_index=self.field_index,
        )

        self.assertCountEqual(expected_output, converted_output)
