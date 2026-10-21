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

from recidiviz.common.constants.state.state_charge import StateChargeV2Status
from recidiviz.common.constants.state.state_sentence import (
    StateSentenceType,
    StateSentencingAuthority,
)
from recidiviz.common.constants.state.state_supervision_violation import (
    StateSupervisionViolationType,
)
from recidiviz.persistence.entity.activity import entities
from recidiviz.persistence.entity.activity.entities import (
    StateIncarcerationPeriod,
    StateSupervisionViolatedConditionEntry,
    StateSupervisionViolation,
    StateSupervisionViolationResponse,
    StateSupervisionViolationResponseDecisionEntry,
    StateSupervisionViolationTypeEntry,
)
from recidiviz.persistence.entity.activity.normalized_entities import (
    NormalizedStateChargeV2,
    NormalizedStateIncarcerationPeriod,
    NormalizedStateSentence,
    NormalizedStateSupervisionCaseTypeEntry,
    NormalizedStateSupervisionViolation,
    NormalizedStateSupervisionViolationResponse,
)
from recidiviz.persistence.entity.activity.normalized_entities_utils import (
    AdditionalAttributesMap,
)
from recidiviz.pipelines.ingest.activity.normalization.normalized_entity_conversion_utils import (
    convert_entity_trees_to_normalized_versions,
    fields_unique_to_normalized_class,
)
from recidiviz.tests.persistence.entity.activity.entities_test_utils import (
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
                admission_date=datetime.date(2020, 1, 1),
                release_date=datetime.date(2020, 2, 1),
            ),
            StateIncarcerationPeriod.new_with_defaults(
                state_code="US_XX",
                incarceration_period_id=222,
                external_id="ip2",
                admission_date=datetime.date(2020, 2, 1),
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
                admission_date=datetime.date(2020, 1, 1),
                release_date=datetime.date(2020, 2, 1),
                purpose_for_incarceration_subtype="XYZ",
                incarceration_admission_violation_type=StateSupervisionViolationType.TECHNICAL,
            ),
            NormalizedStateIncarcerationPeriod(
                state_code="US_XX",
                incarceration_period_id=222,
                external_id="ip2",
                sequence_num=1,
                admission_date=datetime.date(2020, 2, 1),
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
        charge = entities.StateChargeV2.new_with_defaults(
            charge_v2_id=1,
            state_code="US_XX",
            external_id="c1",
            status=StateChargeV2Status.PRESENT_WITHOUT_INFO,
        )
        sentence1 = entities.StateSentence.new_with_defaults(
            state_code="US_XX",
            external_id="s1",
            sentence_id=1,
            sentence_type=StateSentenceType.STATE_PRISON,
            sentencing_authority=StateSentencingAuthority.STATE,
            charges=[charge],
        )
        sentence2 = entities.StateSentence.new_with_defaults(
            state_code="US_XX",
            external_id="s2",
            sentence_id=2,
            sentence_type=StateSentenceType.STATE_PRISON,
            sentencing_authority=StateSentencingAuthority.STATE,
            charges=[charge],
        )

        charge.sentences = [sentence1, sentence2]

        normalized_trees = convert_entity_trees_to_normalized_versions(
            root_entities=[sentence1, sentence2],
            normalized_entity_class=NormalizedStateSentence,
            additional_attributes_map={
                entities.StateSentence.__name__: {
                    1: {
                        "sentence_inferred_group_id": None,
                        "sentence_imposed_group_id": None,
                    },
                    2: {
                        "sentence_inferred_group_id": None,
                        "sentence_imposed_group_id": None,
                    },
                },
                entities.StateChargeV2.__name__: {},
            },
        )

        expected_charge = NormalizedStateChargeV2(
            charge_v2_id=1,
            state_code="US_XX",
            external_id="c1",
            status=StateChargeV2Status.PRESENT_WITHOUT_INFO,
        )
        expected_sentence1 = NormalizedStateSentence(
            state_code="US_XX",
            external_id="s1",
            sentence_id=1,
            sentence_type=StateSentenceType.STATE_PRISON,
            sentencing_authority=StateSentencingAuthority.STATE,
            sentence_inferred_group_id=None,
            sentence_imposed_group_id=None,
            charges=[expected_charge],
        )
        expected_sentence2 = NormalizedStateSentence(
            state_code="US_XX",
            external_id="s2",
            sentence_id=2,
            sentence_type=StateSentenceType.STATE_PRISON,
            sentencing_authority=StateSentencingAuthority.STATE,
            sentence_inferred_group_id=None,
            sentence_imposed_group_id=None,
            charges=[expected_charge],
        )
        expected_charge.sentences = [expected_sentence1, expected_sentence2]
        self.assertListEqual(
            [expected_sentence1, expected_sentence2],
            normalized_trees,
        )

    def test_convert_entity_trees_to_normalized_versions_empty_list(self) -> None:
        normalized_trees = convert_entity_trees_to_normalized_versions(
            root_entities=[],
            normalized_entity_class=NormalizedStateIncarcerationPeriod,
            additional_attributes_map={},
        )

        self.assertEqual([], normalized_trees)
