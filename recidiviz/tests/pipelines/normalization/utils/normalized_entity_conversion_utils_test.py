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

from recidiviz.common.constants.state.state_charge import StateChargeStatus
from recidiviz.common.constants.state.state_incarceration_period import (
    StateSpecializedPurposeForIncarceration,
)
from recidiviz.common.constants.state.state_sentence import StateSentenceStatus
from recidiviz.common.constants.state.state_supervision_violation import (
    StateSupervisionViolationType,
)
from recidiviz.persistence.entity.normalized_entities_utils import (
    LEGACY_NORMALIZATION_ENTITY_CLASSES,
    AdditionalAttributesMap,
    get_shared_additional_attributes_map_for_entities,
)
from recidiviz.persistence.entity.state import entities, normalized_entities
from recidiviz.persistence.entity.state.entities import (
    StateCharge,
    StateEarlyDischarge,
    StateIncarcerationPeriod,
    StateSupervisionSentence,
    StateSupervisionViolatedConditionEntry,
    StateSupervisionViolation,
    StateSupervisionViolationResponse,
    StateSupervisionViolationResponseDecisionEntry,
    StateSupervisionViolationTypeEntry,
)
from recidiviz.persistence.entity.state.normalized_entities import (
    NormalizedStateCharge,
    NormalizedStateEarlyDischarge,
    NormalizedStateIncarcerationPeriod,
    NormalizedStateSupervisionCaseTypeEntry,
    NormalizedStateSupervisionSentence,
    NormalizedStateSupervisionViolatedConditionEntry,
    NormalizedStateSupervisionViolation,
    NormalizedStateSupervisionViolationResponse,
)
from recidiviz.pipelines.normalization.utils.normalized_entity_conversion_utils import (
    bq_schema_for_normalized_state_entity,
    convert_entities_to_normalized_dicts,
    convert_entity_trees_to_normalized_versions,
    fields_unique_to_normalized_class,
)
from recidiviz.tests.persistence.entity.state.entities_test_utils import (
    get_normalized_violation_tree,
    get_violation_tree,
)


class TestBQSchemaForNormalizedStateEntity(unittest.TestCase):
    """Tests the bq_schema_for_normalized_state_entity function."""

    def test_bq_schema_for_normalized_state_entity(self) -> None:
        """Test that we can call this function for all NormalizedStateEntity entities
        without crashing"""
        for entity in LEGACY_NORMALIZATION_ENTITY_CLASSES:
            _ = bq_schema_for_normalized_state_entity(entity)

    def test_bq_schema_for_normalized_state_entity_test_output(self) -> None:
        schema_for_entity = bq_schema_for_normalized_state_entity(
            NormalizedStateSupervisionViolatedConditionEntry
        )

        expected_schema = [
            SchemaField(
                "supervision_violated_condition_entry_id",
                bigquery.enums.SqlTypeNames.INTEGER.value,
            ),
            SchemaField("state_code", bigquery.enums.SqlTypeNames.STRING.value),
            SchemaField("condition", "STRING", "NULLABLE"),
            SchemaField("condition_raw_text", bigquery.enums.SqlTypeNames.STRING.value),
            SchemaField("person_id", bigquery.enums.SqlTypeNames.INTEGER.value),
            SchemaField(
                "supervision_violation_id",
                bigquery.enums.SqlTypeNames.INTEGER.value,
            ),
        ]

        self.assertEqual(expected_schema, schema_for_entity)

    def test_bq_schema_for_normalized_state_entity_extra_attributes(self) -> None:
        """Tests that fields that exist on a NormalizedStateEntity but not on the
        base entity are included in the schema columns."""
        normalized_ip = normalized_entities.NormalizedStateIncarcerationPeriod
        base_ip = StateIncarcerationPeriod

        normalized_ip_field_names = set(attr.fields_dict(normalized_ip).keys())
        base_ip_field_names = set(attr.fields_dict(base_ip).keys())  # type: ignore[arg-type]
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

        expected_unique_fields = {
            "sequence_num",
            "purpose_for_incarceration_subtype",
            "incarceration_admission_violation_type",
        }

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
        )

        self.assertEqual([get_normalized_violation_tree()], normalized_trees)

    def test_convert_entity_trees_to_normalized_versions_ips(self) -> None:
        ips = [
            StateIncarcerationPeriod.new_with_defaults(
                state_code="US_XX",
                incarceration_period_id=111,
                external_id="ip1",
            ),
            StateIncarcerationPeriod.new_with_defaults(
                state_code="US_XX",
                incarceration_period_id=222,
                external_id="ip2",
            ),
        ]

        additional_attributes_map = {
            StateIncarcerationPeriod.__name__: {
                111: {
                    "sequence_num": 0,
                    "purpose_for_incarceration_subtype": "XYZ",
                    "incarceration_admission_violation_type": StateSupervisionViolationType.TECHNICAL,
                },
                222: {"sequence_num": 1, "purpose_for_incarceration_subtype": "AAA"},
            }
        }

        normalized_trees = convert_entity_trees_to_normalized_versions(
            root_entities=ips,
            normalized_entity_class=NormalizedStateIncarcerationPeriod,
            additional_attributes_map=additional_attributes_map,
        )

        expected_normalized_ips = [
            NormalizedStateIncarcerationPeriod(
                state_code="US_XX",
                incarceration_period_id=111,
                external_id="ip1",
                sequence_num=0,
                purpose_for_incarceration_subtype="XYZ",
                incarceration_admission_violation_type=StateSupervisionViolationType.TECHNICAL,
            ),
            NormalizedStateIncarcerationPeriod(
                state_code="US_XX",
                incarceration_period_id=222,
                external_id="ip2",
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
        )

        expected_normalized_svrs = (
            get_normalized_violation_tree().supervision_violation_responses
        )
        for svr in expected_normalized_svrs:
            svr.supervision_violation = None
        self.assertEqual(expected_normalized_svrs, normalized_trees)

    @mock.patch(
        "recidiviz.persistence.entity."
        "normalized_entities_utils.LEGACY_NORMALIZATION_ENTITY_CLASSES",
        [
            NormalizedStateSupervisionSentence,
            NormalizedStateCharge,
            NormalizedStateEarlyDischarge,
        ],
    )
    def test_convert_entity_trees_to_normalized_versions_many_to_many_entities(
        self,
    ) -> None:
        charge = entities.StateCharge.new_with_defaults(
            charge_id=1,
            state_code="US_XX",
            external_id="c1",
            status=StateChargeStatus.PRESENT_WITHOUT_INFO,
        )
        early_discharge = entities.StateEarlyDischarge.new_with_defaults(
            early_discharge_id=1,
            state_code="US_XX",
            external_id="ed1",
        )
        ss1 = entities.StateSupervisionSentence.new_with_defaults(
            state_code="US_XX",
            external_id="ss1",
            status=StateSentenceStatus.PRESENT_WITHOUT_INFO,
            supervision_sentence_id=1,
            charges=[charge],
            early_discharges=[early_discharge],
        )
        ss2 = entities.StateSupervisionSentence.new_with_defaults(
            state_code="US_XX",
            external_id="ss2",
            status=StateSentenceStatus.PRESENT_WITHOUT_INFO,
            supervision_sentence_id=2,
            charges=[charge],
        )
        early_discharge.supervision_sentence = ss1

        charge.supervision_sentences = [ss1, ss2]

        normalized_trees = convert_entity_trees_to_normalized_versions(
            root_entities=[ss1, ss2],
            normalized_entity_class=NormalizedStateSupervisionSentence,
            additional_attributes_map={
                entities.StateSupervisionSentence.__name__: {},
                entities.StateCharge.__name__: {},
                entities.StateEarlyDischarge.__name__: {},
            },
        )

        expected_charge = NormalizedStateCharge(
            charge_id=1,
            state_code="US_XX",
            external_id="c1",
            status=StateChargeStatus.PRESENT_WITHOUT_INFO,
        )
        expected_early_discharge = NormalizedStateEarlyDischarge(
            early_discharge_id=1,
            state_code="US_XX",
            external_id="ed1",
        )
        expected_ss1 = NormalizedStateSupervisionSentence(
            state_code="US_XX",
            external_id="ss1",
            supervision_sentence_id=1,
            status=StateSentenceStatus.PRESENT_WITHOUT_INFO,
            charges=[expected_charge],
            early_discharges=[expected_early_discharge],
        )
        expected_ss2 = NormalizedStateSupervisionSentence(
            state_code="US_XX",
            external_id="ss2",
            supervision_sentence_id=2,
            status=StateSentenceStatus.PRESENT_WITHOUT_INFO,
            charges=[expected_charge],
            early_discharges=[],
        )
        expected_charge.supervision_sentences = [expected_ss1, expected_ss2]
        expected_early_discharge.supervision_sentence = expected_ss1
        self.assertListEqual(
            [expected_ss1, expected_ss2],
            normalized_trees,
        )

    def test_convert_entity_trees_to_normalized_versions_empty_list(self) -> None:
        normalized_trees = convert_entity_trees_to_normalized_versions(
            root_entities=[],
            normalized_entity_class=NormalizedStateIncarcerationPeriod,
            additional_attributes_map={},
        )

        self.assertEqual([], normalized_trees)


class TestConvertEntitiesToNormalizedDicts(unittest.TestCase):
    """Tests the convert_entities_to_normalized_dicts function."""

    def test_convert_entities_to_normalized_dicts(self) -> None:
        person_id = 123

        entities_to_convert = [
            StateIncarcerationPeriod.new_with_defaults(
                state_code="US_XX",
                incarceration_period_id=111,
                external_id="ip1",
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
                    "external_id": "ip1",
                    "facility": None,
                    "housing_unit": None,
                    "housing_unit_category": None,
                    "housing_unit_category_raw_text": None,
                    "housing_unit_type": None,
                    "housing_unit_type_raw_text": None,
                    "incarceration_period_id": 111,
                    "incarceration_type": None,
                    "incarceration_type_raw_text": None,
                    "release_date": None,
                    "release_reason": None,
                    "release_reason_raw_text": None,
                    "custody_level": None,
                    "custody_level_raw_text": None,
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
            root_entity_id=person_id,
            root_entity_id_name="person_id",
            state_code="US_XX",
            entities=entities_to_convert,
            additional_attributes_map=additional_attributes_map,
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
                    "external_id": "sv1",
                    "is_sex_offense": None,
                    "is_violent": False,
                    "violation_metadata": None,
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
                    "condition": "SPECIAL_CONDITIONS",
                    "condition_raw_text": "MISSED CURFEW",
                    "person_id": 123,
                    "state_code": "US_XX",
                    "supervision_violated_condition_entry_id": 3,
                    "supervision_violation_id": 1,
                },
            ),
            (
                StateSupervisionViolationResponseDecisionEntry.__name__,
                {
                    "decision": "EXTERNAL_UNKNOWN",
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
                    "deciding_staff_external_id": None,
                    "deciding_staff_external_id_type": None,
                    "external_id": "svr1",
                    "is_draft": None,
                    "violation_response_metadata": None,
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
                    "decision": "PRIVILEGES_REVOKED",
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
                    "deciding_staff_external_id": None,
                    "deciding_staff_external_id_type": None,
                    "external_id": "svr2",
                    "is_draft": None,
                    "violation_response_metadata": None,
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
            root_entity_id=person_id,
            root_entity_id_name="person_id",
            state_code="US_XX",
            entities=entities_to_convert,
            additional_attributes_map=additional_attributes_map,
        )

        self.assertCountEqual(expected_output, converted_output)

    def test_convert_entities_to_normalized_dicts_many_to_many(self) -> None:
        person_id = 123

        supervision_sentence1 = StateSupervisionSentence.new_with_defaults(
            state_code="US_XX",
            external_id="ss1",
            supervision_sentence_id=111,
            status=StateSentenceStatus.PRESENT_WITHOUT_INFO,
        )

        supervision_sentence2 = StateSupervisionSentence.new_with_defaults(
            state_code="US_XX",
            external_id="ss2",
            supervision_sentence_id=222,
            status=StateSentenceStatus.PRESENT_WITHOUT_INFO,
        )

        supervision_sentence3 = StateSupervisionSentence.new_with_defaults(
            state_code="US_XX",
            external_id="ss3",
            supervision_sentence_id=333,
            status=StateSentenceStatus.PRESENT_WITHOUT_INFO,
        )

        charge1 = StateCharge.new_with_defaults(
            charge_id=1,
            external_id="c1",
            state_code="US_XX",
            status=StateChargeStatus.PRESENT_WITHOUT_INFO,
            supervision_sentences=[supervision_sentence1, supervision_sentence2],
        )

        charge2 = StateCharge.new_with_defaults(
            charge_id=2,
            external_id="c2",
            state_code="US_XX",
            status=StateChargeStatus.PRESENT_WITHOUT_INFO,
            supervision_sentences=[supervision_sentence1, supervision_sentence2],
        )

        early_discharge = StateEarlyDischarge.new_with_defaults(
            early_discharge_id=1,
            external_id="ed1",
            state_code="US_XX",
            supervision_sentence=supervision_sentence1,
        )

        supervision_sentence1.charges = [charge1, charge2]
        supervision_sentence2.charges = [charge1, charge2]
        supervision_sentence1.early_discharges = [early_discharge]

        entities_to_convert = [
            supervision_sentence1,
            supervision_sentence2,
            supervision_sentence3,
        ]

        additional_attributes_map: AdditionalAttributesMap = (
            get_shared_additional_attributes_map_for_entities(entities_to_convert)
        )

        converted_output = convert_entities_to_normalized_dicts(
            root_entity_id=person_id,
            root_entity_id_name="person_id",
            state_code="US_XX",
            entities=entities_to_convert,
            additional_attributes_map=additional_attributes_map,
        )

        expected_output = [
            (
                f"{StateCharge.__name__}_{StateSupervisionSentence.__name__}",
                {
                    "charge_id": 1,
                    "supervision_sentence_id": 111,
                    "state_code": "US_XX",
                },
            ),
            (
                f"{StateCharge.__name__}_{StateSupervisionSentence.__name__}",
                {
                    "charge_id": 2,
                    "supervision_sentence_id": 111,
                    "state_code": "US_XX",
                },
            ),
            (
                StateSupervisionSentence.__name__,
                {
                    "supervision_sentence_id": 111,
                    "external_id": "ss1",
                    "status": "PRESENT_WITHOUT_INFO",
                    "status_raw_text": None,
                    "supervision_type": None,
                    "supervision_type_raw_text": None,
                    "date_imposed": None,
                    "effective_date": None,
                    "projected_completion_date": None,
                    "completion_date": None,
                    "is_life": None,
                    "state_code": "US_XX",
                    "county_code": None,
                    "min_length_days": None,
                    "max_length_days": None,
                    "sentence_metadata": None,
                    "conditions": None,
                    "person_id": 123,
                },
            ),
            (
                f"{StateCharge.__name__}_{StateSupervisionSentence.__name__}",
                {
                    "charge_id": 1,
                    "supervision_sentence_id": 222,
                    "state_code": "US_XX",
                },
            ),
            (
                f"{StateCharge.__name__}_{StateSupervisionSentence.__name__}",
                {
                    "charge_id": 2,
                    "supervision_sentence_id": 222,
                    "state_code": "US_XX",
                },
            ),
            (
                StateSupervisionSentence.__name__,
                {
                    "supervision_sentence_id": 222,
                    "external_id": "ss2",
                    "status": "PRESENT_WITHOUT_INFO",
                    "status_raw_text": None,
                    "supervision_type": None,
                    "supervision_type_raw_text": None,
                    "date_imposed": None,
                    "effective_date": None,
                    "projected_completion_date": None,
                    "completion_date": None,
                    "is_life": None,
                    "state_code": "US_XX",
                    "county_code": None,
                    "min_length_days": None,
                    "max_length_days": None,
                    "sentence_metadata": None,
                    "conditions": None,
                    "person_id": 123,
                },
            ),
            (
                StateCharge.__name__,
                {
                    "charge_id": 1,
                    "external_id": "c1",
                    "status": "PRESENT_WITHOUT_INFO",
                    "status_raw_text": None,
                    "offense_date": None,
                    "date_charged": None,
                    "state_code": "US_XX",
                    "county_code": None,
                    "ncic_code": None,
                    "statute": None,
                    "description": None,
                    "attempted": None,
                    "classification_type": None,
                    "classification_type_raw_text": None,
                    "classification_subtype": None,
                    "offense_type": None,
                    "is_violent": None,
                    "is_sex_offense": None,
                    "is_drug": None,
                    "counts": None,
                    "charge_notes": None,
                    "charging_entity": None,
                    "is_controlling": None,
                    "judge_full_name": None,
                    "judge_external_id": None,
                    "judicial_district_code": None,
                    "person_id": 123,
                },
            ),
            (
                StateCharge.__name__,
                {
                    "charge_id": 2,
                    "external_id": "c2",
                    "status": "PRESENT_WITHOUT_INFO",
                    "status_raw_text": None,
                    "offense_date": None,
                    "date_charged": None,
                    "state_code": "US_XX",
                    "county_code": None,
                    "ncic_code": None,
                    "statute": None,
                    "description": None,
                    "attempted": None,
                    "classification_type": None,
                    "classification_type_raw_text": None,
                    "classification_subtype": None,
                    "offense_type": None,
                    "is_violent": None,
                    "is_sex_offense": None,
                    "is_drug": None,
                    "counts": None,
                    "charge_notes": None,
                    "charging_entity": None,
                    "is_controlling": None,
                    "judge_full_name": None,
                    "judge_external_id": None,
                    "judicial_district_code": None,
                    "person_id": 123,
                },
            ),
            (
                StateEarlyDischarge.__name__,
                {
                    "county_code": None,
                    "deciding_body_type": None,
                    "deciding_body_type_raw_text": None,
                    "decision": None,
                    "decision_date": None,
                    "decision_raw_text": None,
                    "decision_status": None,
                    "decision_status_raw_text": None,
                    "early_discharge_id": 1,
                    "external_id": "ed1",
                    "incarceration_sentence_id": None,
                    "person_id": 123,
                    "request_date": None,
                    "requesting_body_type": None,
                    "requesting_body_type_raw_text": None,
                    "state_code": "US_XX",
                    "supervision_sentence_id": 111,
                },
            ),
            (
                StateSupervisionSentence.__name__,
                {
                    "supervision_sentence_id": 333,
                    "external_id": "ss3",
                    "status": "PRESENT_WITHOUT_INFO",
                    "status_raw_text": None,
                    "supervision_type": None,
                    "supervision_type_raw_text": None,
                    "date_imposed": None,
                    "effective_date": None,
                    "projected_completion_date": None,
                    "completion_date": None,
                    "is_life": None,
                    "state_code": "US_XX",
                    "county_code": None,
                    "min_length_days": None,
                    "max_length_days": None,
                    "sentence_metadata": None,
                    "conditions": None,
                    "person_id": 123,
                },
            ),
        ]

        self.assertCountEqual(expected_output, converted_output)
