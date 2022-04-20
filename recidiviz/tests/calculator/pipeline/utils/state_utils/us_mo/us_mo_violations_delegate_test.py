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
"""Tests the us_mo_violation_delegate.py file."""
import unittest
from datetime import date
from typing import List

from recidiviz.calculator.pipeline.metrics.utils.violation_utils import (
    _shorthand_for_violation_subtype,
    filter_violation_responses_for_violation_history,
    sorted_violation_subtypes_by_severity,
    violation_type_from_subtype,
    violation_type_subtypes_with_violation_type_mappings,
)
from recidiviz.calculator.pipeline.utils.state_utils.us_mo.us_mo_violations_delegate import (
    _UNSUPPORTED_VIOLATION_SUBTYPE_VALUES,
    _VIOLATION_TYPE_AND_SUBTYPE_SHORTHAND_ORDERED_MAP,
    UsMoViolationDelegate,
)
from recidiviz.common.constants.state.state_supervision_violation import (
    StateSupervisionViolationType,
)
from recidiviz.common.constants.state.state_supervision_violation_response import (
    StateSupervisionViolationResponseType,
)
from recidiviz.common.constants.states import StateCode
from recidiviz.persistence.entity.state.entities import (
    StateSupervisionViolatedConditionEntry,
    StateSupervisionViolation,
    StateSupervisionViolationResponse,
    StateSupervisionViolationTypeEntry,
)

_STATE_CODE = StateCode.US_MO.value


class TestFilterViolationResponses(unittest.TestCase):
    """Tests the filter_violation_responses_for_violation_history function when the UsMoViolationDelegate is provided"""

    def setUp(self) -> None:
        self.delegate = UsMoViolationDelegate()

    def _test_filter_violation_responses(
        self,
        violation_responses: List[StateSupervisionViolationResponse],
        include_follow_up_responses: bool = False,
    ) -> List[StateSupervisionViolationResponse]:
        return filter_violation_responses_for_violation_history(
            self.delegate,
            violation_responses,
            include_follow_up_responses,
        )

    def test_filter_violation_responses_INI(self) -> None:
        supervision_violation_responses = [
            StateSupervisionViolationResponse.new_with_defaults(
                state_code=_STATE_CODE,
                response_date=date(2021, 1, 1),
                response_type=StateSupervisionViolationResponseType.CITATION,
            ),
            StateSupervisionViolationResponse.new_with_defaults(
                state_code=_STATE_CODE,
                response_date=date(2021, 1, 1),
                response_type=StateSupervisionViolationResponseType.VIOLATION_REPORT,
                response_subtype="INI",  # Should be included
            ),
        ]

        filtered_responses = self._test_filter_violation_responses(
            supervision_violation_responses, include_follow_up_responses=True
        )

        self.assertEqual(supervision_violation_responses, filtered_responses)

    def test_filter_violation_responses_ITR(self) -> None:
        supervision_violation_responses = [
            StateSupervisionViolationResponse.new_with_defaults(
                state_code=_STATE_CODE,
                response_date=date(2021, 1, 1),
                response_type=StateSupervisionViolationResponseType.CITATION,
            ),
            StateSupervisionViolationResponse.new_with_defaults(
                state_code=_STATE_CODE,
                response_date=date(2021, 1, 1),
                response_type=StateSupervisionViolationResponseType.VIOLATION_REPORT,
                response_subtype="ITR",  # Should be included
            ),
        ]

        filtered_responses = self._test_filter_violation_responses(
            supervision_violation_responses, include_follow_up_responses=True
        )

        self.assertEqual(supervision_violation_responses, filtered_responses)

    def test_filter_violation_responses_do_not_include_supplemental(self) -> None:
        supervision_violation_responses = [
            StateSupervisionViolationResponse.new_with_defaults(
                state_code=_STATE_CODE,
                response_date=date(2021, 1, 1),
                response_type=StateSupervisionViolationResponseType.CITATION,
            ),
            StateSupervisionViolationResponse.new_with_defaults(
                state_code=_STATE_CODE,
                response_date=date(2021, 1, 1),
                response_type=StateSupervisionViolationResponseType.VIOLATION_REPORT,
                response_subtype="SUP",  # Should not be included
            ),
        ]

        filtered_responses = self._test_filter_violation_responses(
            supervision_violation_responses, include_follow_up_responses=False
        )
        expected_output = [
            StateSupervisionViolationResponse.new_with_defaults(
                state_code=_STATE_CODE,
                response_date=date(2021, 1, 1),
                response_type=StateSupervisionViolationResponseType.CITATION,
            )
        ]

        self.assertEqual(expected_output, filtered_responses)

    def test_filter_violation_responses_do_include_supplemental(self) -> None:
        supervision_violation_responses = [
            StateSupervisionViolationResponse.new_with_defaults(
                state_code=_STATE_CODE,
                response_date=date(2021, 1, 1),
                response_type=StateSupervisionViolationResponseType.CITATION,
            ),
            StateSupervisionViolationResponse.new_with_defaults(
                state_code=_STATE_CODE,
                response_date=date(2021, 1, 1),
                response_type=StateSupervisionViolationResponseType.VIOLATION_REPORT,
                response_subtype="SUP",  # Should be included
            ),
        ]

        filtered_responses = self._test_filter_violation_responses(
            supervision_violation_responses, include_follow_up_responses=True
        )
        expected_output = [
            StateSupervisionViolationResponse.new_with_defaults(
                state_code=_STATE_CODE,
                response_date=date(2021, 1, 1),
                response_type=StateSupervisionViolationResponseType.CITATION,
            ),
            StateSupervisionViolationResponse.new_with_defaults(
                state_code=_STATE_CODE,
                response_date=date(2021, 1, 1),
                response_type=StateSupervisionViolationResponseType.VIOLATION_REPORT,
                response_subtype="SUP",  # Should be included
            ),
        ]

        self.assertEqual(expected_output, filtered_responses)

    def test_filter_violation_responses_none_valid(self) -> None:
        supervision_violation_responses = [
            StateSupervisionViolationResponse.new_with_defaults(
                state_code=_STATE_CODE,
                response_date=date(2021, 1, 1),
                response_type=StateSupervisionViolationResponseType.VIOLATION_REPORT,
                response_subtype="SUP",  # Should not be included
            ),
            StateSupervisionViolationResponse.new_with_defaults(
                state_code=_STATE_CODE,
                response_date=date(2021, 1, 1),
                response_type=StateSupervisionViolationResponseType.VIOLATION_REPORT,
                response_subtype="HOF",  # Should not be included
            ),
        ]

        filtered_responses = self._test_filter_violation_responses(
            supervision_violation_responses, include_follow_up_responses=False
        )
        expected_output: List[StateSupervisionViolationResponse] = []

        self.assertEqual(expected_output, filtered_responses)

    def test_filter_violation_responses_unexpected_subtype(self) -> None:
        supervision_violation_responses = [
            StateSupervisionViolationResponse.new_with_defaults(
                state_code=_STATE_CODE,
                response_date=date(2021, 1, 1),
                response_type=StateSupervisionViolationResponseType.VIOLATION_REPORT,
                response_subtype="XXX",  # Not one we expect to see
            )
        ]

        with self.assertRaises(ValueError):
            _ = self._test_filter_violation_responses(
                supervision_violation_responses, include_follow_up_responses=True
            )

    def test_filter_violation_responses_no_date(self) -> None:
        supervision_violation_responses = [
            StateSupervisionViolationResponse.new_with_defaults(
                state_code=_STATE_CODE,
                response_type=StateSupervisionViolationResponseType.VIOLATION_REPORT,
                response_subtype="SUP",  # Should not be included
            ),
            StateSupervisionViolationResponse.new_with_defaults(
                state_code=_STATE_CODE,
                response_date=date(2021, 1, 1),
                response_type=StateSupervisionViolationResponseType.CITATION,
            ),
        ]

        filtered_responses = self._test_filter_violation_responses(
            supervision_violation_responses, include_follow_up_responses=False
        )
        expected_output: List[StateSupervisionViolationResponse] = [
            StateSupervisionViolationResponse.new_with_defaults(
                state_code=_STATE_CODE,
                response_date=date(2021, 1, 1),
                response_type=StateSupervisionViolationResponseType.CITATION,
            )
        ]

        self.assertEqual(expected_output, filtered_responses)


class TestUsMoGetViolationTypeSubstringsForViolation(unittest.TestCase):
    """Tests the get_violation_type_subtype_strings_for_violation function when the UsMoViolationDelegate is provided."""

    def setUp(self) -> None:
        self.delegate = UsMoViolationDelegate()

    def test_us_mo_get_violation_type_subtype_strings_for_violation(self) -> None:
        # Arrange
        violation = StateSupervisionViolation.new_with_defaults(
            state_code=_STATE_CODE,
            supervision_violation_types=[
                StateSupervisionViolationTypeEntry.new_with_defaults(
                    state_code=_STATE_CODE,
                    violation_type=StateSupervisionViolationType.FELONY,
                )
            ],
        )

        # Act
        type_subtype_strings = (
            self.delegate.get_violation_type_subtype_strings_for_violation(violation)
        )

        # Assert
        expected_type_subtype_strings = ["FELONY"]
        self.assertEqual(expected_type_subtype_strings, type_subtype_strings)

    def test_us_mo_get_violation_type_subtype_strings_for_violation_substance(
        self,
    ) -> None:
        # Arrange
        violation = StateSupervisionViolation.new_with_defaults(
            state_code=_STATE_CODE,
            supervision_violation_types=[
                StateSupervisionViolationTypeEntry.new_with_defaults(
                    state_code=_STATE_CODE,
                    violation_type=StateSupervisionViolationType.TECHNICAL,
                )
            ],
            supervision_violated_conditions=[
                StateSupervisionViolatedConditionEntry.new_with_defaults(
                    state_code=_STATE_CODE, condition="DRG"
                )
            ],
        )

        # Act
        type_subtype_strings = (
            self.delegate.get_violation_type_subtype_strings_for_violation(violation)
        )

        # Assert
        expected_type_subtype_strings = ["SUBSTANCE_ABUSE"]
        self.assertEqual(expected_type_subtype_strings, type_subtype_strings)

    def test_us_mo_get_violation_type_subtype_strings_for_violation_law_citation(
        self,
    ) -> None:
        # Arrange
        violation = StateSupervisionViolation.new_with_defaults(
            state_code=_STATE_CODE,
            supervision_violation_types=[
                StateSupervisionViolationTypeEntry.new_with_defaults(
                    state_code=_STATE_CODE,
                    violation_type=StateSupervisionViolationType.TECHNICAL,
                )
            ],
            supervision_violated_conditions=[
                StateSupervisionViolatedConditionEntry.new_with_defaults(
                    state_code=_STATE_CODE, condition="LAW_CITATION"
                )
            ],
        )

        # Act
        type_subtype_strings = (
            self.delegate.get_violation_type_subtype_strings_for_violation(violation)
        )

        # Assert
        expected_type_subtype_strings = ["LAW_CITATION"]
        self.assertEqual(expected_type_subtype_strings, type_subtype_strings)

    def test_us_mo_get_violation_type_subtype_strings_for_violation_technical(
        self,
    ) -> None:
        # Arrange
        violation = StateSupervisionViolation.new_with_defaults(
            state_code=_STATE_CODE,
            supervision_violation_types=[
                StateSupervisionViolationTypeEntry.new_with_defaults(
                    state_code=_STATE_CODE,
                    violation_type=StateSupervisionViolationType.TECHNICAL,
                )
            ],
            supervision_violated_conditions=[
                StateSupervisionViolatedConditionEntry.new_with_defaults(
                    state_code=_STATE_CODE, condition="EMP"
                )
            ],
        )

        # Act
        type_subtype_strings = (
            self.delegate.get_violation_type_subtype_strings_for_violation(violation)
        )

        # Assert
        expected_type_subtype_strings = ["EMP", "TECHNICAL"]
        self.assertEqual(expected_type_subtype_strings, type_subtype_strings)

    def test_us_mo_get_violation_type_subtype_strings_for_violation_no_types(
        self,
    ) -> None:
        # Arrange
        violation = StateSupervisionViolation.new_with_defaults(state_code=_STATE_CODE)

        # Act
        type_subtype_strings = (
            self.delegate.get_violation_type_subtype_strings_for_violation(violation)
        )

        # Assert
        expected_type_subtype_strings: List[str] = []
        self.assertEqual(expected_type_subtype_strings, type_subtype_strings)

    def test_violation_type_subtypes_with_violation_type_mappings(self) -> None:
        supported_violation_subtypes = (
            violation_type_subtypes_with_violation_type_mappings(self.delegate)
        )
        expected_violation_subtypes = {
            "FELONY",
            "MISDEMEANOR",
            "LAW_CITATION",
            "ABSCONDED",
            "MUNICIPAL",
            "ESCAPED",
            "SUBSTANCE_ABUSE",
            "TECHNICAL",
            "INTERNAL_UNKNOWN",
            "EXTERNAL_UNKNOWN",
        }
        self.assertEqual(supported_violation_subtypes, expected_violation_subtypes)


class TestUsMoSortedViolationSubtypesBySeverity(unittest.TestCase):
    """Tests the sorted_violation_subtypes_by_severity function when the UsMoViolationDelegate is provided."""

    def setUp(self) -> None:
        self.delegate = UsMoViolationDelegate()

    def test_us_mo_sorted_violation_subtypes_by_severity(self) -> None:
        violation_subtypes = ["TECHNICAL", "FELONY", "ABSCONDED"]

        sorted_subtypes = sorted_violation_subtypes_by_severity(
            violation_subtypes, self.delegate
        )

        expected_sorted_subtypes = ["FELONY", "ABSCONDED", "TECHNICAL"]

        self.assertEqual(expected_sorted_subtypes, sorted_subtypes)

    def test_us_mo_sorted_violation_subtypes_by_severity_law_citation(self) -> None:
        violation_subtypes = ["ABSCONDED", "LAW_CITATION"]

        sorted_subtypes = sorted_violation_subtypes_by_severity(
            violation_subtypes, self.delegate
        )

        expected_sorted_subtypes = ["LAW_CITATION", "ABSCONDED"]

        self.assertEqual(expected_sorted_subtypes, sorted_subtypes)

    def test_us_mo_sorted_violation_subtypes_by_severity_substance(self) -> None:
        violation_subtypes = ["EMP", "SUBSTANCE_ABUSE", "SPC"]

        sorted_subtypes = sorted_violation_subtypes_by_severity(
            violation_subtypes, self.delegate
        )

        expected_sorted_subtypes = ["SUBSTANCE_ABUSE", "EMP", "SPC"]

        self.assertEqual(expected_sorted_subtypes, sorted_subtypes)


class TestUsMoViolationUtilsSubtypeFunctions(unittest.TestCase):
    """Tests multiple functions in violation_utils related to violation subtypes when the UsMoViolationDelegate is provided."""

    def setUp(self) -> None:
        self.delegate = UsMoViolationDelegate()

    def test_us_mo_violation_type_from_subtype(self) -> None:
        # Assert that all of the StateSupervisionViolationType raw values map to their corresponding violation_type
        for violation_type in StateSupervisionViolationType:
            if violation_type.value not in _UNSUPPORTED_VIOLATION_SUBTYPE_VALUES:
                violation_type_from_subtype_test = violation_type_from_subtype(
                    self.delegate, violation_type.value
                )

                self.assertEqual(violation_type, violation_type_from_subtype_test)

    def test_us_mo_violation_type_from_subtype_law_citation(self) -> None:
        violation_subtype = "LAW_CITATION"

        violation_type_from_subtype_test = violation_type_from_subtype(
            self.delegate, violation_subtype
        )

        self.assertEqual(
            StateSupervisionViolationType.TECHNICAL, violation_type_from_subtype_test
        )

    def test_us_mo_violation_type_from_subtype_substance_abuse(self) -> None:
        violation_subtype = "SUBSTANCE_ABUSE"

        violation_type_from_subtype_test = violation_type_from_subtype(
            self.delegate, violation_subtype
        )

        self.assertEqual(
            StateSupervisionViolationType.TECHNICAL, violation_type_from_subtype_test
        )

    def test_us_mo_shorthand_for_violation_subtype(self) -> None:
        # Assert that all of the StateSupervisionViolationType values are supported
        for violation_type in StateSupervisionViolationType:
            if violation_type.value not in _UNSUPPORTED_VIOLATION_SUBTYPE_VALUES:
                _ = _shorthand_for_violation_subtype(
                    self.delegate, violation_type.value
                )

    def test_violationTypeAndSubtypeShorthandMap_isComplete(self) -> None:
        all_types_subtypes = [
            violation_type
            for violation_type, _, _ in _VIOLATION_TYPE_AND_SUBTYPE_SHORTHAND_ORDERED_MAP
        ]

        for violation_type in StateSupervisionViolationType:
            if violation_type.value not in _UNSUPPORTED_VIOLATION_SUBTYPE_VALUES:
                self.assertTrue(violation_type in all_types_subtypes)
