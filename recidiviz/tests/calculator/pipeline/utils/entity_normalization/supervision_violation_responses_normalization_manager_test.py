# Recidiviz - a data platform for criminal justice reform
# Copyright (C) 2021 Recidiviz, Inc.
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
"""Tests for the violation_response_normalization_manager.py file."""
import datetime
import unittest
from typing import List

import attr

from recidiviz.calculator.pipeline.utils.entity_normalization.supervision_violation_responses_normalization_manager import (
    ViolationResponseNormalizationManager,
)
from recidiviz.calculator.pipeline.utils.state_utils.templates.us_xx.us_xx_violation_response_normalization_delegate import (
    UsXxViolationResponseNormalizationDelegate,
)
from recidiviz.common.constants.state.state_supervision_violation import (
    StateSupervisionViolationType,
)
from recidiviz.common.constants.state.state_supervision_violation_response import (
    StateSupervisionViolationResponseType,
)
from recidiviz.persistence.entity.state.entities import (
    StateSupervisionViolatedConditionEntry,
    StateSupervisionViolation,
    StateSupervisionViolationResponse,
    StateSupervisionViolationTypeEntry,
)


class TestPrepareViolationResponsesForCalculations(unittest.TestCase):
    """State-agnostic tests for pre-processing that happens to all violation
    responses regardless of state (dropping null dates, dropping drafts, sorting)."""

    def setUp(self) -> None:
        self.state_code = "US_XX"
        self.delegate = UsXxViolationResponseNormalizationDelegate()
        self.person_id = 9900000123

    def _normalized_violation_responses_for_calculations(
        self,
        violation_responses: List[StateSupervisionViolationResponse],
    ) -> List[StateSupervisionViolationResponse]:
        entity_normalization_manager = ViolationResponseNormalizationManager(
            person_id=self.person_id,
            violation_responses=violation_responses,
            delegate=self.delegate,
        )

        (
            processed_vrs,
            _,
        ) = (
            entity_normalization_manager.normalized_violation_responses_for_calculations()
        )

        return processed_vrs

    def test_default_filtered_violation_responses_for_violation_history_draft(self):
        placeholder_violation = StateSupervisionViolation.new_with_defaults(
            state_code=self.state_code,
        )

        violation_responses = [
            StateSupervisionViolationResponse.new_with_defaults(
                state_code=self.state_code,
                supervision_violation_response_id=123,
                response_type=StateSupervisionViolationResponseType.VIOLATION_REPORT,
                is_draft=True,
                response_date=datetime.date(2000, 1, 1),
                supervision_violation=placeholder_violation,
            ),
            StateSupervisionViolationResponse.new_with_defaults(
                state_code=self.state_code,
                supervision_violation_response_id=456,
                response_type=StateSupervisionViolationResponseType.VIOLATION_REPORT,
                response_date=datetime.date(1998, 2, 1),
                supervision_violation=placeholder_violation,
            ),
            StateSupervisionViolationResponse.new_with_defaults(
                state_code=self.state_code,
                supervision_violation_response_id=789,
                response_type=StateSupervisionViolationResponseType.VIOLATION_REPORT,
                response_date=datetime.date(1997, 3, 1),
                supervision_violation=placeholder_violation,
            ),
        ]

        placeholder_violation.supervision_violation_responses = violation_responses

        normalized_responses = self._normalized_violation_responses_for_calculations(
            violation_responses=violation_responses
        )

        expected_placeholder_violation = StateSupervisionViolation.new_with_defaults(
            state_code=self.state_code,
        )

        expected_violation_responses = [
            StateSupervisionViolationResponse.new_with_defaults(
                state_code=self.state_code,
                supervision_violation_response_id=789,
                response_type=StateSupervisionViolationResponseType.VIOLATION_REPORT,
                response_date=datetime.date(1997, 3, 1),
                supervision_violation=expected_placeholder_violation,
            ),
            StateSupervisionViolationResponse.new_with_defaults(
                state_code=self.state_code,
                supervision_violation_response_id=456,
                response_type=StateSupervisionViolationResponseType.VIOLATION_REPORT,
                response_date=datetime.date(1998, 2, 1),
                supervision_violation=expected_placeholder_violation,
            ),
        ]

        expected_placeholder_violation.supervision_violation_responses = (
            expected_violation_responses
        )

        self.assertEqual(
            expected_violation_responses,
            normalized_responses,
        )

    def test_default_filtered_violation_responses_for_violation_history_null_date(self):
        placeholder_violation = StateSupervisionViolation.new_with_defaults(
            state_code=self.state_code,
        )

        violation_responses = [
            StateSupervisionViolationResponse.new_with_defaults(
                state_code=self.state_code,
                supervision_violation_response_id=123,
                response_type=StateSupervisionViolationResponseType.VIOLATION_REPORT,
                supervision_violation=placeholder_violation,
            ),
            StateSupervisionViolationResponse.new_with_defaults(
                state_code=self.state_code,
                supervision_violation_response_id=456,
                response_type=StateSupervisionViolationResponseType.VIOLATION_REPORT,
                response_date=datetime.date(1998, 2, 1),
                supervision_violation=placeholder_violation,
            ),
            StateSupervisionViolationResponse.new_with_defaults(
                state_code=self.state_code,
                supervision_violation_response_id=789,
                response_type=StateSupervisionViolationResponseType.VIOLATION_REPORT,
                response_date=datetime.date(1997, 3, 1),
                supervision_violation=placeholder_violation,
            ),
        ]

        placeholder_violation.supervision_violation_responses = violation_responses

        normalized_responses = self._normalized_violation_responses_for_calculations(
            violation_responses=violation_responses
        )

        expected_placeholder_violation = StateSupervisionViolation.new_with_defaults(
            state_code=self.state_code,
        )

        expected_violation_responses = [
            StateSupervisionViolationResponse.new_with_defaults(
                state_code=self.state_code,
                supervision_violation_response_id=789,
                response_type=StateSupervisionViolationResponseType.VIOLATION_REPORT,
                response_date=datetime.date(1997, 3, 1),
                supervision_violation=expected_placeholder_violation,
            ),
            StateSupervisionViolationResponse.new_with_defaults(
                state_code=self.state_code,
                supervision_violation_response_id=456,
                response_type=StateSupervisionViolationResponseType.VIOLATION_REPORT,
                response_date=datetime.date(1998, 2, 1),
                supervision_violation=expected_placeholder_violation,
            ),
        ]

        expected_placeholder_violation.supervision_violation_responses = (
            expected_violation_responses
        )

        self.assertEqual(
            expected_violation_responses,
            normalized_responses,
        )

    def test_update_violations_on_responses(self) -> None:
        # Arrange
        supervision_violation = StateSupervisionViolation.new_with_defaults(
            state_code=self.state_code,
            supervision_violation_id=123,
            supervision_violated_conditions=[
                StateSupervisionViolatedConditionEntry.new_with_defaults(
                    state_code=self.state_code, condition="LAW"
                ),
            ],
        )

        supervision_violation_response = (
            StateSupervisionViolationResponse.new_with_defaults(
                state_code=self.state_code,
                supervision_violation_response_id=456,
                response_type=StateSupervisionViolationResponseType.CITATION,
                supervision_violation=supervision_violation,
                response_date=datetime.date(2022, 1, 4),
            )
        )

        # Hydrate bidirectional relationships
        for condition in supervision_violation.supervision_violated_conditions:
            condition.supervision_violation = supervision_violation
        supervision_violation.supervision_violation_responses = [
            supervision_violation_response
        ]

        vr_copy = attr.evolve(supervision_violation_response)

        # Act
        normalized_responses = self._normalized_violation_responses_for_calculations(
            violation_responses=[supervision_violation_response]
        )

        # Assert
        # There should be no updating of responses with the state-agnostic default
        # functionality
        self.assertEqual([vr_copy], normalized_responses)

    def test_sorted_violation_responses(self) -> None:
        # Arrange
        supervision_violation = StateSupervisionViolation.new_with_defaults(
            supervision_violation_id=123,
            external_id="123",
            state_code=self.state_code,
            supervision_violation_types=[
                StateSupervisionViolationTypeEntry.new_with_defaults(
                    state_code=self.state_code,
                    violation_type=StateSupervisionViolationType.ABSCONDED,
                ),
            ],
        )

        ssvr = StateSupervisionViolationResponse.new_with_defaults(
            state_code=self.state_code,
            external_id="123",
            supervision_violation_response_id=123,
            supervision_violation=supervision_violation,
            response_date=datetime.date(2008, 12, 1),
            response_type=StateSupervisionViolationResponseType.PERMANENT_DECISION,
        )

        other_supervision_violation = StateSupervisionViolation.new_with_defaults(
            supervision_violation_id=456,
            external_id="456",
            state_code=self.state_code,
            supervision_violation_types=[
                StateSupervisionViolationTypeEntry.new_with_defaults(
                    state_code=self.state_code,
                    violation_type=StateSupervisionViolationType.FELONY,
                ),
            ],
        )

        other_ssvr = StateSupervisionViolationResponse.new_with_defaults(
            state_code=self.state_code,
            external_id="456",
            supervision_violation_response_id=456,
            supervision_violation=other_supervision_violation,
            response_date=datetime.date(2008, 12, 25),
            response_type=StateSupervisionViolationResponseType.PERMANENT_DECISION,
        )

        # Hydrate bidirectional relationships
        for violation in [
            supervision_violation,
            other_supervision_violation,
        ]:
            for type_entry in violation.supervision_violation_types:
                type_entry.supervision_violation = violation
        supervision_violation.supervision_violation_responses = [ssvr]
        other_supervision_violation.supervision_violation_responses = [other_ssvr]

        violation_responses = [other_ssvr, ssvr]

        # Act
        normalized_responses = self._normalized_violation_responses_for_calculations(
            violation_responses=violation_responses
        )

        # Assert
        self.assertEqual(
            [ssvr, other_ssvr],
            normalized_responses,
        )

    def test_sorted_violation_responses_additional_attributes(self) -> None:
        # Arrange
        supervision_violation = StateSupervisionViolation.new_with_defaults(
            supervision_violation_id=123,
            external_id="123",
            state_code=self.state_code,
            supervision_violation_types=[
                StateSupervisionViolationTypeEntry.new_with_defaults(
                    state_code=self.state_code,
                    violation_type=StateSupervisionViolationType.ABSCONDED,
                ),
            ],
        )

        ssvr = StateSupervisionViolationResponse.new_with_defaults(
            state_code=self.state_code,
            external_id="123",
            supervision_violation_response_id=123,
            supervision_violation=supervision_violation,
            response_date=datetime.date(2008, 12, 1),
            response_type=StateSupervisionViolationResponseType.PERMANENT_DECISION,
        )

        other_supervision_violation = StateSupervisionViolation.new_with_defaults(
            supervision_violation_id=456,
            external_id="456",
            state_code=self.state_code,
            supervision_violation_types=[
                StateSupervisionViolationTypeEntry.new_with_defaults(
                    state_code=self.state_code,
                    violation_type=StateSupervisionViolationType.FELONY,
                ),
            ],
        )

        other_ssvr = StateSupervisionViolationResponse.new_with_defaults(
            state_code=self.state_code,
            external_id="456",
            supervision_violation_response_id=456,
            supervision_violation=other_supervision_violation,
            response_date=datetime.date(2008, 12, 25),
            response_type=StateSupervisionViolationResponseType.PERMANENT_DECISION,
        )

        # Hydrate bidirectional relationships
        for violation in [
            supervision_violation,
            other_supervision_violation,
        ]:
            for type_entry in violation.supervision_violation_types:
                type_entry.supervision_violation = violation
        supervision_violation.supervision_violation_responses = [ssvr]
        other_supervision_violation.supervision_violation_responses = [other_ssvr]

        violation_responses = [other_ssvr, ssvr]

        # Act
        additional_attributes = ViolationResponseNormalizationManager.additional_attributes_map_for_normalized_vrs(
            violation_responses=self._normalized_violation_responses_for_calculations(
                violation_responses=violation_responses
            )
        )

        expected_additional_attributes = {
            StateSupervisionViolationResponse.__name__: {
                123: {"sequence_num": 0},
                456: {"sequence_num": 1},
            }
        }

        # Assert
        self.assertEqual(expected_additional_attributes, additional_attributes)


class TestValidateVrInvariants(unittest.TestCase):
    """Tests the validate_Vr_invariants function."""

    def setUp(self) -> None:
        self.state_code = "US_XX"

        self.supervision_violation = StateSupervisionViolation.new_with_defaults(
            supervision_violation_id=123,
            external_id="123",
            state_code=self.state_code,
            supervision_violation_types=[
                StateSupervisionViolationTypeEntry.new_with_defaults(
                    state_code=self.state_code,
                    violation_type=StateSupervisionViolationType.ABSCONDED,
                ),
            ],
        )

        self.ssvr = StateSupervisionViolationResponse.new_with_defaults(
            state_code=self.state_code,
            external_id="123",
            supervision_violation_response_id=123,
            supervision_violation=self.supervision_violation,
            response_date=datetime.date(2008, 12, 1),
            response_type=StateSupervisionViolationResponseType.PERMANENT_DECISION,
        )

        hydrate_bidirectional_relationships_on_expected_response(self.ssvr)

    def test_validate_vr_invariants_valid(self):
        # Assert no error
        ViolationResponseNormalizationManager.validate_vr_invariants([self.ssvr])

    def test_validate_vr_invariants_invalid_ids(self):
        supervision_violation = StateSupervisionViolation.new_with_defaults(
            supervision_violation_id=456,
            external_id="123",
            state_code=self.state_code,
            supervision_violation_types=[
                StateSupervisionViolationTypeEntry.new_with_defaults(
                    state_code=self.state_code,
                    violation_type=StateSupervisionViolationType.ABSCONDED,
                ),
            ],
        )

        bad_ssvr = StateSupervisionViolationResponse.new_with_defaults(
            state_code=self.state_code,
            external_id="123",
            supervision_violation_response_id=123,
            supervision_violation=supervision_violation,
            response_date=datetime.date(2008, 12, 1),
            response_type=StateSupervisionViolationResponseType.PERMANENT_DECISION,
        )

        hydrate_bidirectional_relationships_on_expected_response(bad_ssvr)

        with self.assertRaises(ValueError) as e:
            ViolationResponseNormalizationManager.validate_vr_invariants(
                [bad_ssvr, self.ssvr]
            )

        self.assertEqual(
            "Finalized list of StateSupervisionViolationResponse contains "
            "duplicate supervision_violation_response_id values: [[123, 123]].",
            e.exception.args[0],
        )

    def test_validate_vr_invariants_invalid_bad_tree(self):
        other_supervision_violation = StateSupervisionViolation.new_with_defaults(
            supervision_violation_id=456,
            external_id="123",
            state_code=self.state_code,
            supervision_violation_types=[
                StateSupervisionViolationTypeEntry.new_with_defaults(
                    state_code=self.state_code,
                    violation_type=StateSupervisionViolationType.ABSCONDED,
                ),
            ],
        )

        bad_ssvr = StateSupervisionViolationResponse.new_with_defaults(
            state_code=self.state_code,
            external_id="123",
            supervision_violation_response_id=678,
            supervision_violation=other_supervision_violation,
            response_date=datetime.date(2008, 12, 1),
            response_type=StateSupervisionViolationResponseType.PERMANENT_DECISION,
        )

        hydrate_bidirectional_relationships_on_expected_response(bad_ssvr)

        # supervision_violation is connected to the bad_ssvr, but the bad_ssvr is
        # pointing to the other_supervision_violation
        self.supervision_violation.supervision_violation_responses.append(bad_ssvr)

        with self.assertRaises(ValueError) as e:
            ViolationResponseNormalizationManager.validate_vr_invariants(
                [bad_ssvr, self.ssvr]
            )

        self.assertTrue(
            "Violation response normalization resulted in an invalid entity tree, "
            "where a child StateSupervisionViolationResponse is not pointing to its "
            "parent StateSupervisionViolation" in e.exception.args[0]
        )

    def test_validate_vr_invariants_invalid_bad_tree_not_dropped(self):
        bad_ssvr = StateSupervisionViolationResponse.new_with_defaults(
            state_code=self.state_code,
            external_id="123",
            supervision_violation_response_id=678,
            supervision_violation=self.supervision_violation,
            response_date=datetime.date(2008, 12, 1),
            response_type=StateSupervisionViolationResponseType.PERMANENT_DECISION,
        )

        hydrate_bidirectional_relationships_on_expected_response(bad_ssvr)

        # supervision_violation is connected to the bad_ssvr, but the bad_ssvr is
        # not in the final list of responses
        self.supervision_violation.supervision_violation_responses.append(bad_ssvr)

        with self.assertRaises(ValueError) as e:
            ViolationResponseNormalizationManager.validate_vr_invariants([self.ssvr])

        self.assertTrue(
            "Violation response normalization resulted in an invalid "
            "entity tree, where a StateSupervisionViolationResponse was "
            "dropped from the list during normalization, but was not "
            "disconnected from its parent violation: " in e.exception.args[0]
        )


def hydrate_bidirectional_relationships_on_expected_response(
    expected_response: StateSupervisionViolationResponse,
) -> None:
    """Hydrates all bi-directional relationships in the
    StateSupervisionViolationResponse subtree. For use in tests that need the full
    entity graph to be connected."""
    if expected_response.supervision_violation:
        for (
            type_entry
        ) in expected_response.supervision_violation.supervision_violation_types:
            type_entry.supervision_violation = expected_response.supervision_violation
        for (
            condition
        ) in expected_response.supervision_violation.supervision_violated_conditions:
            condition.supervision_violation = expected_response.supervision_violation

        expected_response.supervision_violation.supervision_violation_responses = [
            expected_response
        ]

    for decision in expected_response.supervision_violation_response_decisions:
        decision.supervision_violation_response = expected_response
