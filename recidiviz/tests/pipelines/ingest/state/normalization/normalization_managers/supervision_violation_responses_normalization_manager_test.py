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

from recidiviz.common.constants.state.state_supervision_violated_condition import (
    StateSupervisionViolatedConditionType,
)
from recidiviz.common.constants.state.state_supervision_violation import (
    StateSupervisionViolationType,
)
from recidiviz.common.constants.state.state_supervision_violation_response import (
    StateSupervisionViolationResponseDecision,
    StateSupervisionViolationResponseType,
)
from recidiviz.persistence.entity.state.entities import (
    StateSupervisionViolatedConditionEntry,
    StateSupervisionViolation,
    StateSupervisionViolationResponse,
    StateSupervisionViolationResponseDecisionEntry,
    StateSupervisionViolationTypeEntry,
)
from recidiviz.persistence.entity.state.normalized_entities import (
    NormalizedStateSupervisionViolation,
    NormalizedStateSupervisionViolationResponse,
    NormalizedStateSupervisionViolationResponseDecisionEntry,
    NormalizedStateSupervisionViolationTypeEntry,
)
from recidiviz.pipelines.ingest.state.normalization.normalization_managers.supervision_violation_responses_normalization_manager import (
    StateSpecificViolationResponseNormalizationDelegate,
    ViolationResponseNormalizationManager,
    normalized_violation_responses_from_processed_versions,
)
from recidiviz.pipelines.utils.execution_utils import (
    build_staff_external_id_to_staff_id_map,
)
from recidiviz.pipelines.utils.state_utils.templates.us_xx.us_xx_violation_response_normalization_delegate import (
    UsXxViolationResponseNormalizationDelegate,
)
from recidiviz.tests.persistence.entity.state.entities_test_utils import (
    get_normalized_violation_tree,
    get_violation_tree,
    hydrate_bidirectional_relationships_on_expected_response,
)

STATE_PERSON_TO_STATE_STAFF_LIST = [
    {
        "person_id": 123,
        "staff_id": 10000,
        "staff_external_id": "EMP1",
        "staff_external_id_type": "US_XX_STAFF_ID",
    },
    {
        "person_id": 123,
        "staff_id": 20000,
        "staff_external_id": "EMP2",
        "staff_external_id_type": "US_XX_STAFF_ID",
    },
    {
        "person_id": 123,
        "staff_id": 30000,
        "staff_external_id": "EMP3",
        "staff_external_id_type": "US_XX_STAFF_ID",
    },
]


class TestNormalizedViolationResponsesFromProcessedVersions(unittest.TestCase):
    """Tests the normalized_violation_responses_from_processed_versions function."""

    def setUp(self) -> None:
        self.delegate = UsXxViolationResponseNormalizationDelegate()

    def test_normalized_violation_responses_from_processed_versions(self) -> None:
        violation = get_violation_tree()

        violation_responses = violation.supervision_violation_responses

        normalization_manager = ViolationResponseNormalizationManager(
            person_id=123,
            violation_responses=violation_responses,
            delegate=self.delegate,
            staff_external_id_to_staff_id=build_staff_external_id_to_staff_id_map(
                STATE_PERSON_TO_STATE_STAFF_LIST
            ),
        )

        (
            processed_violation_responses,
            additional_attributes,
        ) = normalization_manager.normalized_violation_responses_for_calculations()

        normalized_responses = normalized_violation_responses_from_processed_versions(
            processed_violation_responses=processed_violation_responses,
            additional_vr_attributes=additional_attributes,
        )

        expected_responses = (
            get_normalized_violation_tree().supervision_violation_responses
        )

        self.assertEqual(expected_responses, normalized_responses)

    def test_normalized_violation_responses_from_processed_versions_multiple_violations(
        self,
    ) -> None:
        violation_1 = get_violation_tree(starting_id_value=123)
        violation_2 = get_violation_tree(starting_id_value=456)

        violation_responses = (
            violation_1.supervision_violation_responses
            + violation_2.supervision_violation_responses
        )

        normalization_manager = ViolationResponseNormalizationManager(
            person_id=123,
            violation_responses=violation_responses,
            delegate=self.delegate,
            staff_external_id_to_staff_id=build_staff_external_id_to_staff_id_map(
                STATE_PERSON_TO_STATE_STAFF_LIST
            ),
        )

        (
            processed_violation_responses,
            additional_attributes,
        ) = normalization_manager.normalized_violation_responses_for_calculations()

        normalized_responses = normalized_violation_responses_from_processed_versions(
            processed_violation_responses=processed_violation_responses,
            additional_vr_attributes=additional_attributes,
        )

        expected_violation_1 = get_normalized_violation_tree(starting_id_value=123)
        expected_violation_2 = get_normalized_violation_tree(starting_id_value=456)

        # Normalization sorts violation responses in this order
        expected_violation_1.supervision_violation_responses[0].sequence_num = 0  # type: ignore[attr-defined]
        expected_violation_2.supervision_violation_responses[0].sequence_num = 1  # type: ignore[attr-defined]
        expected_violation_1.supervision_violation_responses[1].sequence_num = 2  # type: ignore[attr-defined]
        expected_violation_2.supervision_violation_responses[1].sequence_num = 3  # type: ignore[attr-defined]

        expected_responses = (
            expected_violation_1.supervision_violation_responses
            + expected_violation_2.supervision_violation_responses
        )

        self.assertEqual(expected_responses, normalized_responses)

    def test_normalized_violation_responses_from_processed_versions_no_violation(
        self,
    ) -> None:
        violation = get_violation_tree()

        violation_responses = violation.supervision_violation_responses
        for vr in violation_responses:
            vr.supervision_violation = None

        normalization_manager = ViolationResponseNormalizationManager(
            person_id=123,
            violation_responses=violation_responses,
            delegate=self.delegate,
            staff_external_id_to_staff_id=build_staff_external_id_to_staff_id_map(
                STATE_PERSON_TO_STATE_STAFF_LIST
            ),
        )

        additional_attributes = (
            normalization_manager.additional_attributes_map_for_normalized_vrs(
                violation_responses=violation_responses
            )
        )

        with self.assertRaises(ValueError) as e:
            _ = normalized_violation_responses_from_processed_versions(
                processed_violation_responses=violation_responses,
                additional_vr_attributes=additional_attributes,
            )

        self.assertTrue(
            "Found empty supervision_violation on response" in e.exception.args[0]
        )

    def test_normalized_violations_de_duplicate_responses_by_date(self) -> None:
        class _DelegateWithDeduplication(
            StateSpecificViolationResponseNormalizationDelegate
        ):
            def should_de_duplicate_responses_by_date(self) -> bool:
                return True

        supervision_violation = StateSupervisionViolation(
            external_id="sv1",
            supervision_violation_id=1,
            state_code="US_XX",
            supervision_violation_types=[
                StateSupervisionViolationTypeEntry(
                    supervision_violation_type_entry_id=2,
                    state_code="US_XX",
                    violation_type=StateSupervisionViolationType.TECHNICAL,
                    violation_type_raw_text="TECHNICAL",
                ),
            ],
        )

        supervision_violation_response = StateSupervisionViolationResponse(
            external_id="svr1",
            state_code="US_XX",
            supervision_violation_response_id=4,
            response_type=StateSupervisionViolationResponseType.CITATION,
            response_date=datetime.date(year=2004, month=9, day=2),
            supervision_violation=supervision_violation,
            supervision_violation_response_decisions=[
                StateSupervisionViolationResponseDecisionEntry(
                    state_code="US_XX",
                    supervision_violation_response_decision_entry_id=5,
                    decision=StateSupervisionViolationResponseDecision.EXTERNAL_UNKNOWN,
                    decision_raw_text="X",
                )
            ],
        )
        supervision_violation.supervision_violation_responses.append(
            supervision_violation_response
        )

        supervision_violation_dup = StateSupervisionViolation(
            external_id="sv1_dup",
            supervision_violation_id=6,
            state_code="US_XX",
            supervision_violation_types=[
                StateSupervisionViolationTypeEntry(
                    supervision_violation_type_entry_id=7,
                    state_code="US_XX",
                    violation_type=StateSupervisionViolationType.TECHNICAL,
                    violation_type_raw_text="TECHNICAL",
                ),
            ],
        )

        supervision_violation_response_dup = StateSupervisionViolationResponse(
            external_id="svr1_dup",
            state_code="US_XX",
            supervision_violation_response_id=9,
            response_type=StateSupervisionViolationResponseType.CITATION,
            response_date=datetime.date(year=2004, month=9, day=2),
            supervision_violation=supervision_violation_dup,
            supervision_violation_response_decisions=[
                StateSupervisionViolationResponseDecisionEntry(
                    state_code="US_XX",
                    supervision_violation_response_decision_entry_id=10,
                    decision=StateSupervisionViolationResponseDecision.EXTERNAL_UNKNOWN,
                    decision_raw_text="X",
                )
            ],
        )
        supervision_violation_dup.supervision_violation_responses.append(
            supervision_violation_response_dup
        )

        violation_responses = [
            supervision_violation_response,
            supervision_violation_response_dup,
        ]

        normalization_manager = ViolationResponseNormalizationManager(
            person_id=123,
            violation_responses=violation_responses,
            delegate=_DelegateWithDeduplication(),
            staff_external_id_to_staff_id=build_staff_external_id_to_staff_id_map(
                STATE_PERSON_TO_STATE_STAFF_LIST
            ),
        )

        expected_normalized_supervision_violation = NormalizedStateSupervisionViolation(
            external_id="sv1",
            supervision_violation_id=9065654955711543502,
            state_code="US_XX",
            supervision_violation_types=[
                NormalizedStateSupervisionViolationTypeEntry(
                    supervision_violation_type_entry_id=9033946637077724355,
                    state_code="US_XX",
                    violation_type=StateSupervisionViolationType.TECHNICAL,
                    violation_type_raw_text="TECHNICAL",
                ),
            ],
        )

        expected_normalized_supervision_violation_response = NormalizedStateSupervisionViolationResponse(
            external_id="svr1",
            state_code="US_XX",
            supervision_violation_response_id=4,
            sequence_num=0,
            response_type=StateSupervisionViolationResponseType.CITATION,
            response_date=datetime.date(year=2004, month=9, day=2),
            supervision_violation=expected_normalized_supervision_violation,
            supervision_violation_response_decisions=[
                NormalizedStateSupervisionViolationResponseDecisionEntry(
                    state_code="US_XX",
                    supervision_violation_response_decision_entry_id=5,
                    decision=StateSupervisionViolationResponseDecision.EXTERNAL_UNKNOWN,
                    decision_raw_text="X",
                )
            ],
        )
        expected_normalized_supervision_violation.supervision_violation_responses.append(
            expected_normalized_supervision_violation_response
        )
        expected_normalized_supervision_violation.supervision_violation_types[
            0
        ].supervision_violation = expected_normalized_supervision_violation
        expected_normalized_supervision_violation_response.supervision_violation_response_decisions[
            0
        ].supervision_violation_response = (
            expected_normalized_supervision_violation_response
        )

        normalized_violations = normalization_manager.get_normalized_violations()

        self.assertEqual(
            [expected_normalized_supervision_violation], normalized_violations
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
            staff_external_id_to_staff_id=build_staff_external_id_to_staff_id_map(
                STATE_PERSON_TO_STATE_STAFF_LIST
            ),
        )

        (
            processed_vrs,
            _,
        ) = (
            entity_normalization_manager.normalized_violation_responses_for_calculations()
        )

        return processed_vrs

    def test_single_violation(self) -> None:
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
            deciding_staff_external_id="EMP1",
            deciding_staff_external_id_type="US_XX_STAFF_ID",
        )

        # Hydrate bidirectional relationships
        hydrate_bidirectional_relationships_on_expected_response(ssvr)

        # Act
        entity_normalization_manager = ViolationResponseNormalizationManager(
            person_id=self.person_id,
            violation_responses=[ssvr],
            delegate=self.delegate,
            staff_external_id_to_staff_id=build_staff_external_id_to_staff_id_map(
                STATE_PERSON_TO_STATE_STAFF_LIST
            ),
        )

        (
            processed_svrs,
            additional_attributes,
        ) = (
            entity_normalization_manager.normalized_violation_responses_for_calculations()
        )

        # Assert
        self.assertEqual([ssvr], processed_svrs)
        self.assertEqual(
            {
                StateSupervisionViolationResponse.__name__: {
                    123: {"deciding_staff_id": 10000, "sequence_num": 0}
                }
            },
            additional_attributes,
        )

    def test_default_filtered_violation_responses_for_violation_history_draft(
        self,
    ) -> None:
        sparse_violation = StateSupervisionViolation.new_with_defaults(
            state_code=self.state_code, external_id="sv1"
        )

        violation_responses = [
            StateSupervisionViolationResponse.new_with_defaults(
                state_code=self.state_code,
                external_id="svr1",
                supervision_violation_response_id=123,
                response_type=StateSupervisionViolationResponseType.VIOLATION_REPORT,
                is_draft=True,
                response_date=datetime.date(2000, 1, 1),
                supervision_violation=sparse_violation,
            ),
            StateSupervisionViolationResponse.new_with_defaults(
                state_code=self.state_code,
                external_id="svr2",
                supervision_violation_response_id=456,
                response_type=StateSupervisionViolationResponseType.VIOLATION_REPORT,
                response_date=datetime.date(1998, 2, 1),
                supervision_violation=sparse_violation,
            ),
            StateSupervisionViolationResponse.new_with_defaults(
                state_code=self.state_code,
                external_id="svr3",
                supervision_violation_response_id=789,
                response_type=StateSupervisionViolationResponseType.VIOLATION_REPORT,
                response_date=datetime.date(1997, 3, 1),
                supervision_violation=sparse_violation,
            ),
        ]

        sparse_violation.supervision_violation_responses = violation_responses

        normalized_responses = self._normalized_violation_responses_for_calculations(
            violation_responses=violation_responses
        )

        expected_sparse_violation = StateSupervisionViolation.new_with_defaults(
            state_code=self.state_code,
            external_id="sv1",
        )

        expected_violation_responses = [
            StateSupervisionViolationResponse.new_with_defaults(
                state_code=self.state_code,
                external_id="svr3",
                supervision_violation_response_id=789,
                response_type=StateSupervisionViolationResponseType.VIOLATION_REPORT,
                response_date=datetime.date(1997, 3, 1),
                supervision_violation=expected_sparse_violation,
            ),
            StateSupervisionViolationResponse.new_with_defaults(
                state_code=self.state_code,
                external_id="svr2",
                supervision_violation_response_id=456,
                response_type=StateSupervisionViolationResponseType.VIOLATION_REPORT,
                response_date=datetime.date(1998, 2, 1),
                supervision_violation=expected_sparse_violation,
            ),
        ]

        expected_sparse_violation.supervision_violation_responses = (
            expected_violation_responses
        )

        self.assertEqual(
            expected_violation_responses,
            normalized_responses,
        )

    def test_default_filtered_violation_responses_for_violation_history_null_date(
        self,
    ) -> None:
        sparse_violation = StateSupervisionViolation.new_with_defaults(
            state_code=self.state_code, external_id="sv1"
        )

        violation_responses = [
            StateSupervisionViolationResponse.new_with_defaults(
                state_code=self.state_code,
                external_id="svr1",
                supervision_violation_response_id=123,
                response_type=StateSupervisionViolationResponseType.VIOLATION_REPORT,
                supervision_violation=sparse_violation,
            ),
            StateSupervisionViolationResponse.new_with_defaults(
                state_code=self.state_code,
                external_id="svr2",
                supervision_violation_response_id=456,
                response_type=StateSupervisionViolationResponseType.VIOLATION_REPORT,
                response_date=datetime.date(1998, 2, 1),
                supervision_violation=sparse_violation,
            ),
            StateSupervisionViolationResponse.new_with_defaults(
                state_code=self.state_code,
                external_id="svr3",
                supervision_violation_response_id=789,
                response_type=StateSupervisionViolationResponseType.VIOLATION_REPORT,
                response_date=datetime.date(1997, 3, 1),
                supervision_violation=sparse_violation,
            ),
        ]

        sparse_violation.supervision_violation_responses = violation_responses

        normalized_responses = self._normalized_violation_responses_for_calculations(
            violation_responses=violation_responses
        )

        expected_sparse_violation = StateSupervisionViolation.new_with_defaults(
            state_code=self.state_code,
            external_id="sv1",
        )

        expected_violation_responses = [
            StateSupervisionViolationResponse.new_with_defaults(
                state_code=self.state_code,
                external_id="svr3",
                supervision_violation_response_id=789,
                response_type=StateSupervisionViolationResponseType.VIOLATION_REPORT,
                response_date=datetime.date(1997, 3, 1),
                supervision_violation=expected_sparse_violation,
            ),
            StateSupervisionViolationResponse.new_with_defaults(
                state_code=self.state_code,
                external_id="svr2",
                supervision_violation_response_id=456,
                response_type=StateSupervisionViolationResponseType.VIOLATION_REPORT,
                response_date=datetime.date(1998, 2, 1),
                supervision_violation=expected_sparse_violation,
            ),
        ]

        expected_sparse_violation.supervision_violation_responses = (
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
            external_id="sv1",
            supervision_violation_id=123,
            supervision_violated_conditions=[
                StateSupervisionViolatedConditionEntry.new_with_defaults(
                    state_code=self.state_code,
                    condition=StateSupervisionViolatedConditionType.LAW,
                    condition_raw_text="LAW",
                ),
            ],
        )

        supervision_violation_response = (
            StateSupervisionViolationResponse.new_with_defaults(
                state_code=self.state_code,
                external_id="svr1",
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
            deciding_staff_external_id="EMP2",
            deciding_staff_external_id_type="US_XX_STAFF_ID",
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
        normalization_manager = ViolationResponseNormalizationManager(
            person_id=self.person_id,
            violation_responses=violation_responses,
            delegate=self.delegate,
            staff_external_id_to_staff_id=build_staff_external_id_to_staff_id_map(
                STATE_PERSON_TO_STATE_STAFF_LIST
            ),
        )
        additional_attributes = normalization_manager.additional_attributes_map_for_normalized_vrs(
            violation_responses=self._normalized_violation_responses_for_calculations(
                violation_responses=violation_responses
            )
        )

        expected_additional_attributes = {
            StateSupervisionViolationResponse.__name__: {
                123: {"deciding_staff_id": 20000, "sequence_num": 0},
                456: {"deciding_staff_id": None, "sequence_num": 1},
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

    def test_validate_vr_invariants_valid(self) -> None:
        # Assert no error
        ViolationResponseNormalizationManager.validate_vr_invariants([self.ssvr])

    def test_validate_vr_invariants_invalid_ids(self) -> None:
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

    def test_validate_vr_invariants_invalid_bad_tree(self) -> None:
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

    def test_validate_vr_invariants_invalid_bad_tree_not_dropped(self) -> None:
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
