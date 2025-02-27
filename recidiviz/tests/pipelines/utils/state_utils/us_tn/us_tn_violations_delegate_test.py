#  Recidiviz - a data platform for criminal justice reform
#  Copyright (C) 2023 Recidiviz, Inc.
#
#  This program is free software: you can redistribute it and/or modify
#  it under the terms of the GNU General Public License as published by
#  the Free Software Foundation, either version 3 of the License, or
#  (at your option) any later version.
#
#  This program is distributed in the hope that it will be useful,
#  but WITHOUT ANY WARRANTY; without even the implied warranty of
#  MERCHANTABILITY or FITNESS FOR A PARTICULAR PURPOSE.  See the
#  GNU General Public License for more details.
#
#  You should have received a copy of the GNU General Public License
#  along with this program.  If not, see <https://www.gnu.org/licenses/>.
#  =============================================================================
""""Tests the us_tn_violation_delegate.py file."""
import unittest
from datetime import date
from typing import List

from recidiviz.common.constants.state.state_supervision_violation import (
    StateSupervisionViolationType,
)
from recidiviz.common.constants.state.state_supervision_violation_response import (
    StateSupervisionViolationResponseDecision,
    StateSupervisionViolationResponseType,
)
from recidiviz.common.constants.states import StateCode
from recidiviz.common.date import DateRange
from recidiviz.persistence.entity.state.entities import (
    StateSupervisionViolationResponse,
)
from recidiviz.persistence.entity.state.normalized_entities import (
    NormalizedStateSupervisionViolation,
    NormalizedStateSupervisionViolationResponse,
    NormalizedStateSupervisionViolationResponseDecisionEntry,
    NormalizedStateSupervisionViolationTypeEntry,
)
from recidiviz.pipelines.metrics.utils import violation_utils
from recidiviz.pipelines.metrics.utils.violation_utils import (
    ViolationHistory,
    filter_violation_responses_for_violation_history,
    get_violation_and_response_history,
)
from recidiviz.pipelines.utils.state_utils.us_tn.us_tn_violations_delegate import (
    UsTnViolationDelegate,
)

_STATE_CODE = StateCode.US_TN.value


class TestUsTnViolationsDelegate(unittest.TestCase):
    """Tests the us_tn_violations_delegate."""

    def setUp(self) -> None:
        self.delegate = UsTnViolationDelegate()

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

    def test_filter_violation_responses_include_permanent_decisions_citation_and_violation_report(
        self,
    ) -> None:
        supervision_violation_responses = [
            StateSupervisionViolationResponse.new_with_defaults(
                state_code=_STATE_CODE,
                external_id="svr1",
                response_date=date(2021, 1, 1),
                response_type=StateSupervisionViolationResponseType.PERMANENT_DECISION,  # Should be included
            ),
            StateSupervisionViolationResponse.new_with_defaults(
                state_code=_STATE_CODE,
                external_id="svr2",
                response_date=date(2021, 1, 11),
                response_type=StateSupervisionViolationResponseType.CITATION,  # Should be included
            ),
            StateSupervisionViolationResponse.new_with_defaults(
                state_code=_STATE_CODE,
                external_id="svr3",
                response_date=date(2021, 1, 20),
                response_type=StateSupervisionViolationResponseType.VIOLATION_REPORT,  # Should be included
            ),
            StateSupervisionViolationResponse.new_with_defaults(
                state_code=_STATE_CODE,
                external_id="svr4",
                response_date=date(2021, 1, 30),
                response_type=StateSupervisionViolationResponseType.INTERNAL_UNKNOWN,  # Should not be included
            ),
        ]

        filtered_responses = self._test_filter_violation_responses(
            supervision_violation_responses, include_follow_up_responses=False
        )
        expected_output = [
            StateSupervisionViolationResponse.new_with_defaults(
                state_code=_STATE_CODE,
                external_id="svr1",
                response_date=date(2021, 1, 1),
                response_type=StateSupervisionViolationResponseType.PERMANENT_DECISION,  # Should be included
            ),
            StateSupervisionViolationResponse.new_with_defaults(
                state_code=_STATE_CODE,
                external_id="svr2",
                response_date=date(2021, 1, 11),
                response_type=StateSupervisionViolationResponseType.CITATION,  # Should be included
            ),
            StateSupervisionViolationResponse.new_with_defaults(
                state_code=_STATE_CODE,
                external_id="svr3",
                response_date=date(2021, 1, 20),
                response_type=StateSupervisionViolationResponseType.VIOLATION_REPORT,  # Should be included
            ),
        ]

        self.assertEqual(expected_output, filtered_responses)

    def test_filter_violation_responses_none_valid(self) -> None:
        supervision_violation_responses = [
            StateSupervisionViolationResponse.new_with_defaults(
                state_code=_STATE_CODE,
                external_id="svr1",
                response_date=date(2021, 1, 1),
                response_type=StateSupervisionViolationResponseType.INTERNAL_UNKNOWN,  # Should not be included
            ),
            StateSupervisionViolationResponse.new_with_defaults(
                state_code=_STATE_CODE,
                external_id="svr2",
                response_date=date(2021, 1, 1),
                response_type=StateSupervisionViolationResponseType.EXTERNAL_UNKNOWN,  # Should not be included
            ),
        ]

        filtered_responses = self._test_filter_violation_responses(
            supervision_violation_responses, include_follow_up_responses=False
        )
        expected_output: List[StateSupervisionViolationResponse] = []

        self.assertEqual(expected_output, filtered_responses)


class TestViolationHistoryWindowPreCommitment(unittest.TestCase):
    """Tests the US_TN specific implementation of violation_history_window_relevant_to_critical_date
    function on the UsTnViolationDelegate."""

    def test_us_tn_violation_history_window_relevant_to_critical_date(
        self,
    ) -> None:

        violation_window = (
            UsTnViolationDelegate().violation_history_window_relevant_to_critical_date(
                critical_date=date(2000, 1, 1),
                sorted_and_filtered_violation_responses=[],
                default_violation_history_window_months=12,
            )
        )

        expected_violation_window = DateRange(
            # 24 months before
            lower_bound_inclusive_date=date(1998, 1, 1),
            # 10 days after admission_date
            upper_bound_exclusive_date=date(2000, 1, 11),
        )

        self.assertEqual(expected_violation_window, violation_window)


class TestViolationAndResponseHistory(unittest.TestCase):
    """Tests the US_TN specific implementation of violation_history_window_relevant_to_critical_date and
    should_include_response_in_violation_history functions using the UsTnViolationDelegate to confirm
    how we select most_severe_violation_type in various scenarios."""

    def setUp(self) -> None:
        self.delegate = UsTnViolationDelegate()

    def test_get_violation_and_response_history_with_tn_logic(
        self,
    ) -> None:
        supervision_violation_1 = NormalizedStateSupervisionViolation.new_with_defaults(
            supervision_violation_id=123455,
            external_id="sv1",
            state_code="US_TN",
            violation_date=date(2021, 1, 1),
            supervision_violation_types=[
                NormalizedStateSupervisionViolationTypeEntry.new_with_defaults(
                    state_code="US_XX",
                    violation_type=StateSupervisionViolationType.TECHNICAL,
                ),
            ],
        )

        supervision_violation_2 = NormalizedStateSupervisionViolation.new_with_defaults(
            supervision_violation_id=123456,
            external_id="sv2",
            state_code="US_TN",
            violation_date=date(2021, 1, 20),
            supervision_violation_types=[
                NormalizedStateSupervisionViolationTypeEntry.new_with_defaults(
                    state_code="US_TN",
                    violation_type=StateSupervisionViolationType.MISDEMEANOR,
                ),
            ],
        )

        supervision_violation_responses = [
            NormalizedStateSupervisionViolationResponse.new_with_defaults(
                state_code=_STATE_CODE,
                external_id="svr1",
                response_date=date(2019, 1, 1),
                response_type=StateSupervisionViolationResponseType.VIOLATION_REPORT,  # Should not be included
            ),
            NormalizedStateSupervisionViolationResponse.new_with_defaults(
                state_code=_STATE_CODE,
                external_id="svr2",
                response_date=date(2021, 1, 1),
                response_type=StateSupervisionViolationResponseType.PERMANENT_DECISION,  # Should be included
                supervision_violation_response_decisions=[
                    NormalizedStateSupervisionViolationResponseDecisionEntry.new_with_defaults(
                        state_code="US_TN",
                        decision=StateSupervisionViolationResponseDecision.REVOCATION,
                    ),
                ],
                supervision_violation=supervision_violation_1,
            ),
            NormalizedStateSupervisionViolationResponse.new_with_defaults(
                state_code=_STATE_CODE,
                external_id="svr3",
                response_date=date(2021, 1, 20),
                response_type=StateSupervisionViolationResponseType.VIOLATION_REPORT,  # Should be included
                supervision_violation_response_decisions=[
                    NormalizedStateSupervisionViolationResponseDecisionEntry.new_with_defaults(
                        state_code="US_TN",
                        decision=StateSupervisionViolationResponseDecision.WARRANT_ISSUED,
                    ),
                ],
                supervision_violation=supervision_violation_2,
            ),
            NormalizedStateSupervisionViolationResponse.new_with_defaults(
                state_code=_STATE_CODE,
                external_id="svr4",
                response_date=date(2021, 1, 30),
                response_type=StateSupervisionViolationResponseType.INTERNAL_UNKNOWN,  # Should not be included
            ),
        ]

        filtered_and_sorted_responses = (
            violation_utils.filter_violation_responses_for_violation_history(
                violation_delegate=self.delegate,
                violation_responses=supervision_violation_responses,
                include_follow_up_responses=False,
            )
        )

        violation_history_window = (
            UsTnViolationDelegate().violation_history_window_relevant_to_critical_date(
                critical_date=date(2021, 3, 1),
                sorted_and_filtered_violation_responses=filtered_and_sorted_responses,
                default_violation_history_window_months=12,
            )
        )

        # Get details about the violation and response history leading up to the
        # admission to incarceration
        violation_history_result = get_violation_and_response_history(
            upper_bound_exclusive_date=violation_history_window.upper_bound_exclusive_date,
            lower_bound_inclusive_date_override=violation_history_window.lower_bound_inclusive_date,
            violation_responses_for_history=filtered_and_sorted_responses,
            violation_delegate=self.delegate,
        )

        expected_violation_history_result = ViolationHistory(
            most_severe_violation_type=StateSupervisionViolationType.MISDEMEANOR,
            most_severe_violation_type_subtype=StateSupervisionViolationType.MISDEMEANOR.value,
            most_severe_violation_id=123456,
            violation_history_id_array="123455,123456",
            most_severe_response_decision=StateSupervisionViolationResponseDecision.REVOCATION,
            response_count=2,
            violation_history_description="1misdemeanor;1technical",
            violation_type_frequency_counter=[["TECHNICAL"], ["MISDEMEANOR"]],
        )

        self.assertEqual(expected_violation_history_result, violation_history_result)
