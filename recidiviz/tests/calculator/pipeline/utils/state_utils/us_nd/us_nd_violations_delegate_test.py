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
"""Tests the us_nd_violation_delegate.py file."""
import unittest
from datetime import date
from typing import List

from recidiviz.calculator.pipeline.metrics.utils.violation_utils import (
    filter_violation_responses_for_violation_history,
)
from recidiviz.calculator.pipeline.utils.state_utils.us_nd.us_nd_violations_delegate import (
    UsNdViolationDelegate,
)
from recidiviz.common.constants.state.state_supervision_violation_response import (
    StateSupervisionViolationResponseType,
)
from recidiviz.common.constants.states import StateCode
from recidiviz.persistence.entity.state.entities import (
    StateSupervisionViolationResponse,
)

_STATE_CODE = StateCode.US_ND.value


class TestFilterViolationResponses(unittest.TestCase):
    """Tests the filter_violation_responses_for_violation_history function when the UsNdViolationDelegate is provided"""

    def setUp(self) -> None:
        self.delegate = UsNdViolationDelegate()

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

    def test_filter_violation_responses_PERMANENT(self) -> None:
        supervision_violation_responses = [
            StateSupervisionViolationResponse.new_with_defaults(
                state_code=_STATE_CODE,
                response_date=date(2021, 1, 1),
                response_type=StateSupervisionViolationResponseType.PERMANENT_DECISION,
            ),
        ]

        filtered_responses = self._test_filter_violation_responses(
            supervision_violation_responses, include_follow_up_responses=True
        )
        self.assertEqual(supervision_violation_responses, filtered_responses)

    def test_filter_violation_responses_do_not_include_non_permanent_decisions(
        self,
    ) -> None:
        supervision_violation_responses = [
            StateSupervisionViolationResponse.new_with_defaults(
                state_code=_STATE_CODE,
                response_date=date(2021, 1, 1),
                response_type=StateSupervisionViolationResponseType.PERMANENT_DECISION,
            ),
            StateSupervisionViolationResponse.new_with_defaults(
                state_code=_STATE_CODE,
                response_date=date(2021, 1, 1),
                response_type=StateSupervisionViolationResponseType.CITATION,  # Should not be included
            ),
            StateSupervisionViolationResponse.new_with_defaults(
                state_code=_STATE_CODE,
                response_date=date(2021, 1, 1),
                response_type=StateSupervisionViolationResponseType.VIOLATION_REPORT,  # Should not be included
            ),
        ]

        filtered_responses = self._test_filter_violation_responses(
            supervision_violation_responses, include_follow_up_responses=False
        )
        expected_output = [
            StateSupervisionViolationResponse.new_with_defaults(
                state_code=_STATE_CODE,
                response_date=date(2021, 1, 1),
                response_type=StateSupervisionViolationResponseType.PERMANENT_DECISION,
            )
        ]

        self.assertEqual(expected_output, filtered_responses)

    def test_filter_violation_responses_none_valid(self) -> None:
        supervision_violation_responses = [
            StateSupervisionViolationResponse.new_with_defaults(
                state_code=_STATE_CODE,
                response_date=date(2021, 1, 1),
                response_type=StateSupervisionViolationResponseType.CITATION,  # Should not be included
            ),
            StateSupervisionViolationResponse.new_with_defaults(
                state_code=_STATE_CODE,
                response_date=date(2021, 1, 1),
                response_type=StateSupervisionViolationResponseType.VIOLATION_REPORT,  # Should not be included
            ),
        ]

        filtered_responses = self._test_filter_violation_responses(
            supervision_violation_responses, include_follow_up_responses=False
        )
        expected_output: List[StateSupervisionViolationResponse] = []

        self.assertEqual(expected_output, filtered_responses)
