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
"""Tests the US_MO-specific aspects of the when the state-specific delegate is used in
the ViolationResponseNormalizationManager."""
import datetime
import unittest
from typing import List

import attr

from recidiviz.calculator.pipeline.utils.entity_normalization.supervision_violation_responses_normalization_manager import (
    ViolationResponseNormalizationManager,
)
from recidiviz.calculator.pipeline.utils.state_utils.us_mo.us_mo_violation_response_normalization_delegate import (
    UsMoViolationResponseNormalizationDelegate,
)
from recidiviz.calculator.pipeline.utils.state_utils.us_mo.us_mo_violations_delegate import (
    LAW_CITATION_SUBTYPE_STR,
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
    """Tests the US_MO-specific aspects of the
    normalized_violation_responses_for_calculations function on the
    ViolationResponseNormalizationManager when a
    UsMoViolationResponseNormalizationDelegate is provided."""

    def setUp(self) -> None:
        self.state_code = "US_MO"
        self.delegate = UsMoViolationResponseNormalizationDelegate()

    def _normalized_violation_responses_for_calculations(
        self,
        violation_responses: List[StateSupervisionViolationResponse],
    ) -> List[StateSupervisionViolationResponse]:
        entity_normalization_manager = ViolationResponseNormalizationManager(
            violation_responses=violation_responses,
            delegate=self.delegate,
        )

        return (
            entity_normalization_manager.normalized_violation_responses_for_calculations()
        )

    def test_prepare_violation_responses_for_calculations_law_citation(self) -> None:
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
                response_date=datetime.date(1999, 10, 12),
            )
        )

        expected_response = attr.evolve(
            supervision_violation_response,
            supervision_violation=attr.evolve(
                supervision_violation,
                supervision_violation_types=[
                    StateSupervisionViolationTypeEntry(
                        state_code=self.state_code,
                        violation_type=StateSupervisionViolationType.TECHNICAL,
                        violation_type_raw_text=None,
                    )
                ],
                supervision_violated_conditions=[
                    StateSupervisionViolatedConditionEntry.new_with_defaults(
                        state_code=self.state_code, condition=LAW_CITATION_SUBTYPE_STR
                    ),
                ],
            ),
        )

        # Act
        updated_responses = self._normalized_violation_responses_for_calculations(
            violation_responses=[supervision_violation_response]
        )

        # Assert
        self.assertEqual([expected_response], updated_responses)

    def test_prepare_violation_responses_for_calculations_no_conditions(self) -> None:
        # Arrange
        supervision_violation = StateSupervisionViolation.new_with_defaults(
            state_code=self.state_code,
        )

        supervision_violation_response = (
            StateSupervisionViolationResponse.new_with_defaults(
                state_code=self.state_code,
                response_type=StateSupervisionViolationResponseType.CITATION,
                supervision_violation=supervision_violation,
                response_date=datetime.date(1999, 10, 12),
            )
        )

        expected_response = attr.evolve(
            supervision_violation_response,
            supervision_violation=attr.evolve(
                supervision_violation,
                supervision_violation_types=[
                    StateSupervisionViolationTypeEntry(
                        state_code=self.state_code,
                        violation_type=StateSupervisionViolationType.TECHNICAL,
                        violation_type_raw_text=None,
                    )
                ],
            ),
        )

        # Act
        updated_responses = self._normalized_violation_responses_for_calculations(
            violation_responses=[supervision_violation_response]
        )

        # Assert
        self.assertEqual([expected_response], updated_responses)

    def test_prepare_violation_responses_for_calculations_no_updates(self) -> None:
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
                response_type=StateSupervisionViolationResponseType.VIOLATION_REPORT,
                supervision_violation=supervision_violation,
                response_date=datetime.date(1999, 10, 12),
            )
        )

        expected_response = attr.evolve(supervision_violation_response)

        # Act
        updated_responses = self._normalized_violation_responses_for_calculations(
            violation_responses=[supervision_violation_response]
        )

        # Assert
        self.assertEqual([expected_response], updated_responses)
