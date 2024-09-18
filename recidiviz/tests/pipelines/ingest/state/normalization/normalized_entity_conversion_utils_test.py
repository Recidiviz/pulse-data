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

import unittest
from typing import Set

from recidiviz.common.constants.state.state_charge import StateChargeStatus
from recidiviz.common.constants.state.state_sentence import StateSentenceStatus
from recidiviz.common.constants.state.state_supervision_violation import (
    StateSupervisionViolationType,
)
from recidiviz.persistence.entity.normalized_entities_utils import (
    AdditionalAttributesMap,
)
from recidiviz.persistence.entity.state import entities
from recidiviz.persistence.entity.state.entities import (
    StateIncarcerationPeriod,
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
    NormalizedStateSupervisionViolation,
    NormalizedStateSupervisionViolationResponse,
)
from recidiviz.pipelines.ingest.state.normalization.normalized_entity_conversion_utils import (
    convert_entity_trees_to_normalized_versions,
    fields_unique_to_normalized_class,
)
from recidiviz.tests.persistence.entity.state.entities_test_utils import (
    get_normalized_violation_tree,
    get_violation_tree,
)


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
