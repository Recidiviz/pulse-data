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
"""Tests for the violation_response_preprocessing_manager.py file."""
import datetime
import unittest
from typing import List

import attr

from recidiviz.calculator.pipeline.utils.state_utils.templates.us_xx.us_xx_violation_response_preprocessing_delegate import (
    UsXxViolationResponsePreprocessingDelegate,
)
from recidiviz.calculator.pipeline.utils.supervision_violation_responses_pre_processing_manager import (
    ViolationResponsePreProcessingManager,
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
        self.delegate = UsXxViolationResponsePreprocessingDelegate()

    def _pre_processed_violation_responses_for_calculations(
        self,
        violation_responses: List[StateSupervisionViolationResponse],
    ) -> List[StateSupervisionViolationResponse]:
        pre_processing_manager = ViolationResponsePreProcessingManager(
            violation_responses=violation_responses,
            delegate=self.delegate,
        )

        return (
            pre_processing_manager.pre_processed_violation_responses_for_calculations()
        )

    def test_default_filtered_violation_responses_for_violation_history_draft(self):
        violation_responses = [
            StateSupervisionViolationResponse.new_with_defaults(
                state_code=self.state_code,
                response_type=StateSupervisionViolationResponseType.VIOLATION_REPORT,
                is_draft=True,
                response_date=datetime.date(2000, 1, 1),
            ),
            StateSupervisionViolationResponse.new_with_defaults(
                state_code=self.state_code,
                response_type=StateSupervisionViolationResponseType.VIOLATION_REPORT,
                response_date=datetime.date(1998, 2, 1),
            ),
            StateSupervisionViolationResponse.new_with_defaults(
                state_code=self.state_code,
                response_type=StateSupervisionViolationResponseType.VIOLATION_REPORT,
                response_date=datetime.date(1997, 3, 1),
            ),
        ]

        vr_1_copy = attr.evolve(violation_responses[2])
        vr_2_copy = attr.evolve(violation_responses[1])

        pre_processed_responses = (
            self._pre_processed_violation_responses_for_calculations(
                violation_responses=violation_responses
            )
        )

        self.assertEqual(
            [vr_1_copy, vr_2_copy],
            pre_processed_responses,
        )

    def test_default_filtered_violation_responses_for_violation_history_null_date(self):
        violation_responses = [
            StateSupervisionViolationResponse.new_with_defaults(
                state_code=self.state_code,
                response_type=StateSupervisionViolationResponseType.VIOLATION_REPORT,
            ),
            StateSupervisionViolationResponse.new_with_defaults(
                state_code=self.state_code,
                response_type=StateSupervisionViolationResponseType.VIOLATION_REPORT,
                response_date=datetime.date(1998, 2, 1),
            ),
            StateSupervisionViolationResponse.new_with_defaults(
                state_code=self.state_code,
                response_type=StateSupervisionViolationResponseType.VIOLATION_REPORT,
                response_date=datetime.date(1997, 3, 1),
            ),
        ]

        vr_1_copy = attr.evolve(violation_responses[2])
        vr_2_copy = attr.evolve(violation_responses[1])

        pre_processed_responses = (
            self._pre_processed_violation_responses_for_calculations(
                violation_responses=violation_responses
            )
        )

        self.assertEqual(
            [vr_1_copy, vr_2_copy],
            pre_processed_responses,
        )

    def test_update_violations_on_responses(self) -> None:
        # Arrange
        supervision_violation = StateSupervisionViolation.new_with_defaults(
            state_code=self.state_code,
            supervision_violated_conditions=[
                StateSupervisionViolatedConditionEntry.new_with_defaults(
                    state_code=self.state_code, condition="LAW"
                ),
            ],
        )

        supervision_violation_response = (
            StateSupervisionViolationResponse.new_with_defaults(
                state_code=self.state_code,
                response_type=StateSupervisionViolationResponseType.CITATION,
                supervision_violation=supervision_violation,
                response_date=datetime.date(2022, 1, 4),
            )
        )

        vr_copy = attr.evolve(supervision_violation_response)

        # Act
        pre_processed_responses = (
            self._pre_processed_violation_responses_for_calculations(
                violation_responses=[supervision_violation_response]
            )
        )

        # Assert
        # There should be no updating of responses with the state-agnostic default
        # functionality
        self.assertEqual([vr_copy], pre_processed_responses)

    def test_sorted_violation_responses(self) -> None:
        # Arrange
        supervision_violation = StateSupervisionViolation.new_with_defaults(
            supervision_violation_id=123,
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
            supervision_violation_response_id=123,
            supervision_violation=supervision_violation,
            response_date=datetime.date(2008, 12, 1),
            response_type=StateSupervisionViolationResponseType.PERMANENT_DECISION,
        )

        other_supervision_violation = StateSupervisionViolation.new_with_defaults(
            supervision_violation_id=123,
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
            supervision_violation_response_id=123,
            supervision_violation=other_supervision_violation,
            response_date=datetime.date(2008, 12, 25),
            response_type=StateSupervisionViolationResponseType.PERMANENT_DECISION,
        )

        violation_responses = [other_ssvr, ssvr]

        # Act
        pre_processed_responses = (
            self._pre_processed_violation_responses_for_calculations(
                violation_responses=violation_responses
            )
        )

        # Assert
        self.assertEqual(
            [ssvr, other_ssvr],
            pre_processed_responses,
        )

    def test_responses_without_violations(self) -> None:
        # Arrange
        supervision_violation_response = (
            StateSupervisionViolationResponse.new_with_defaults(
                state_code=self.state_code,
                response_type=StateSupervisionViolationResponseType.PERMANENT_DECISION,
                supervision_violation=None,
                response_date=datetime.date(2022, 1, 4),
            )
        )

        vr_copy = attr.evolve(supervision_violation_response)

        # Act
        pre_processed_responses = (
            self._pre_processed_violation_responses_for_calculations(
                violation_responses=[supervision_violation_response]
            )
        )

        # Assert
        self.assertEqual([vr_copy], pre_processed_responses)
