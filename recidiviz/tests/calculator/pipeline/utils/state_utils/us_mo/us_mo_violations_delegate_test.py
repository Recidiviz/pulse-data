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
from datetime import date
from typing import List

import pytest

from recidiviz.calculator.pipeline.utils.state_utils.us_mo.us_mo_violations_delegate import (
    UsMoViolationDelegate,
)
from recidiviz.calculator.pipeline.utils.violation_utils import (
    filter_violation_responses_for_violation_history,
)
from recidiviz.common.constants.state.state_supervision_violation_response import (
    StateSupervisionViolationResponseType,
)
from recidiviz.common.constants.states import StateCode
from recidiviz.persistence.entity.state.entities import (
    StateSupervisionViolationResponse,
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

        with pytest.raises(ValueError):
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
