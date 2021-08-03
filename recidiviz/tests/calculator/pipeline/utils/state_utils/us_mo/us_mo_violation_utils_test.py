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

import pytest

# TODO(#8106): Delete these imports before closing this task
# pylint: disable=protected-access
from recidiviz.calculator.pipeline.utils.state_utils.us_mo.us_mo_violation_utils import (
    _normalize_violations_on_responses,
    us_mo_shorthand_for_violation_subtype,
    us_mo_violation_type_from_subtype,
)

# pylint: disable=protected-access
from recidiviz.calculator.pipeline.utils.state_utils.us_mo.us_mo_violations_delegate import (
    _LAW_CITATION_SUBTYPE_STR,
    _UNSUPPORTED_VIOLATION_SUBTYPE_VALUES,
    _VIOLATION_TYPE_AND_SUBTYPE_SHORTHAND_ORDERED_MAP,
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


class TestUsMoViolationUtilsSubtypeFunctions(unittest.TestCase):
    """Tests multiple functions in us_mo_violation_utils related to violation subtypes."""

    def test_us_mo_violation_type_from_subtype(self) -> None:
        # Assert that all of the StateSupervisionViolationType raw values map to their corresponding violation_type
        for violation_type in StateSupervisionViolationType:
            if violation_type.value not in _UNSUPPORTED_VIOLATION_SUBTYPE_VALUES:
                violation_type_from_subtype = us_mo_violation_type_from_subtype(
                    violation_type.value
                )

                self.assertEqual(violation_type, violation_type_from_subtype)

    def test_us_mo_violation_type_from_subtype_law_citation(self) -> None:
        violation_subtype = "LAW_CITATION"

        violation_type_from_subtype = us_mo_violation_type_from_subtype(
            violation_subtype
        )

        self.assertEqual(
            StateSupervisionViolationType.TECHNICAL, violation_type_from_subtype
        )

    def test_us_mo_violation_type_from_subtype_substance_abuse(self) -> None:
        violation_subtype = "SUBSTANCE_ABUSE"

        violation_type_from_subtype = us_mo_violation_type_from_subtype(
            violation_subtype
        )

        self.assertEqual(
            StateSupervisionViolationType.TECHNICAL, violation_type_from_subtype
        )

    def test_us_mo_shorthand_for_violation_subtype(self) -> None:
        # Assert that all of the StateSupervisionViolationType values are supported
        for violation_type in StateSupervisionViolationType:
            if violation_type.value not in _UNSUPPORTED_VIOLATION_SUBTYPE_VALUES:
                _ = us_mo_shorthand_for_violation_subtype(violation_type.value)

    def test_violationTypeAndSubtypeShorthandMap_isComplete(self) -> None:
        all_types_subtypes = [
            violation_type
            for violation_type, _, _ in _VIOLATION_TYPE_AND_SUBTYPE_SHORTHAND_ORDERED_MAP
        ]

        for violation_type in StateSupervisionViolationType:
            if violation_type.value not in _UNSUPPORTED_VIOLATION_SUBTYPE_VALUES:
                self.assertTrue(violation_type in all_types_subtypes)


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
        with pytest.raises(ValueError):
            # This function should only be called for responses from US_MO
            _ = _normalize_violations_on_responses(supervision_violation_response)
