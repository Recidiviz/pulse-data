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
"""Tests the us_mo_violation_utils.py file."""
import unittest

# TODO(#8106): Delete these imports before closing this task
# pylint: disable=protected-access
from recidiviz.calculator.pipeline.utils.state_utils.us_mo.us_mo_violation_utils import (
    _normalize_violations_on_responses,
)

# TODO(#8106): Delete these imports before closing this task
# pylint: disable=protected-access
from recidiviz.calculator.pipeline.utils.state_utils.us_mo.us_mo_violations_delegate import (
    _LAW_CITATION_SUBTYPE_STR,
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

_STATE_CODE = "US_MO"


class TestNormalizeViolationsOnResponsesUsMo(unittest.TestCase):
    """Test the _normalize_violations_on_responses function."""

    def test_normalize_violations_on_responses_us_mo(self) -> None:
        # Arrange
        supervision_violation = StateSupervisionViolation.new_with_defaults(
            state_code="US_MO",
            supervision_violated_conditions=[
                StateSupervisionViolatedConditionEntry.new_with_defaults(
                    state_code="US_MO", condition="LAW"
                ),
            ],
        )

        supervision_violation_response = (
            StateSupervisionViolationResponse.new_with_defaults(
                state_code="US_MO",
                response_type=StateSupervisionViolationResponseType.CITATION,
                supervision_violation=supervision_violation,
            )
        )

        # Act
        _ = _normalize_violations_on_responses(supervision_violation_response)

        # Assert
        self.assertEqual(
            supervision_violation.supervision_violation_types,
            [
                StateSupervisionViolationTypeEntry(
                    state_code="US_MO",
                    violation_type=StateSupervisionViolationType.TECHNICAL,
                    violation_type_raw_text=None,
                )
            ],
        )
        self.assertEqual(
            supervision_violation.supervision_violated_conditions,
            [
                StateSupervisionViolatedConditionEntry.new_with_defaults(
                    state_code="US_MO", condition=_LAW_CITATION_SUBTYPE_STR
                ),
            ],
        )

    def test_normalize_violations_on_responses_us_mo_no_conditions(self) -> None:
        # Arrange
        supervision_violation = StateSupervisionViolation.new_with_defaults(
            state_code="US_MO"
        )

        supervision_violation_response = (
            StateSupervisionViolationResponse.new_with_defaults(
                state_code="US_MO",
                response_type=StateSupervisionViolationResponseType.CITATION,
                supervision_violation=supervision_violation,
            )
        )

        # Act
        _ = _normalize_violations_on_responses(supervision_violation_response)

        # Assert
        self.assertEqual(
            [
                StateSupervisionViolationTypeEntry(
                    state_code="US_MO",
                    violation_type=StateSupervisionViolationType.TECHNICAL,
                    violation_type_raw_text=None,
                )
            ],
            supervision_violation.supervision_violation_types,
        )
        self.assertEqual([], supervision_violation.supervision_violated_conditions)

    def test_normalize_violations_on_responses_violation_report_us_mo(self) -> None:
        # Arrange
        supervision_violation = StateSupervisionViolation.new_with_defaults(
            state_code="US_MO",
            supervision_violated_conditions=[
                StateSupervisionViolatedConditionEntry.new_with_defaults(
                    state_code="US_MO", condition="LAW"
                ),
            ],
        )

        supervision_violation_response = (
            StateSupervisionViolationResponse.new_with_defaults(
                state_code="US_MO",
                response_type=StateSupervisionViolationResponseType.VIOLATION_REPORT,
                supervision_violation=supervision_violation,
            )
        )

        # Act
        _ = _normalize_violations_on_responses(supervision_violation_response)

        # Assert
        self.assertEqual(supervision_violation.supervision_violation_types, [])
        self.assertEqual(
            supervision_violation.supervision_violated_conditions,
            [
                StateSupervisionViolatedConditionEntry.new_with_defaults(
                    state_code="US_MO", condition="LAW"
                ),
            ],
        )

    def test_normalize_violations_on_responses_not_us_mo(self) -> None:
        # Arrange
        supervision_violation = StateSupervisionViolation.new_with_defaults(
            state_code="US_NOT_MO",
            supervision_violated_conditions=[
                StateSupervisionViolatedConditionEntry.new_with_defaults(
                    state_code="US_NOT_MO", condition="LAW"
                ),
            ],
        )

        supervision_violation_response = (
            StateSupervisionViolationResponse.new_with_defaults(
                state_code="US_NOT_MO",
                response_type=StateSupervisionViolationResponseType.CITATION,
                supervision_violation=supervision_violation,
            )
        )

        # Act and Assert
        with self.assertRaises(ValueError):
            # This function should only be called for responses from US_MO
            _ = _normalize_violations_on_responses(supervision_violation_response)
