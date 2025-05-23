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
"""Tests the functions in the violation_response_utils.py file."""
import datetime
import unittest
from typing import List

from recidiviz.common.constants.state.state_supervision_violation_response import (
    StateSupervisionViolationResponseDecision,
    StateSupervisionViolationResponseType,
)
from recidiviz.persistence.entity.state.normalized_entities import (
    NormalizedStateSupervisionViolationResponse,
)
from recidiviz.pipelines.metrics.utils.violation_utils import (
    filter_violation_responses_for_violation_history,
)
from recidiviz.pipelines.utils import violation_response_utils
from recidiviz.pipelines.utils.state_utils.templates.us_xx.us_xx_violations_delegate import (
    UsXxViolationDelegate,
)
from recidiviz.pipelines.utils.violation_response_utils import (
    DECISION_SEVERITY_ORDER,
    identify_most_severe_response_decision,
    violation_responses_in_window,
)


class TestResponsesOnMostRecentResponseDate(unittest.TestCase):
    """Tests the responses_on_most_recent_response_date function."""

    @staticmethod
    def _test_responses_on_most_recent_response_date(
        response_dates: List[datetime.date],
    ) -> List[NormalizedStateSupervisionViolationResponse]:
        """Helper function for testing the responses_on_most_recent_response_date function."""
        violation_responses: List[NormalizedStateSupervisionViolationResponse] = []

        for i, response_date in enumerate(response_dates):
            violation_responses.append(
                NormalizedStateSupervisionViolationResponse(
                    supervision_violation_response_id=1,
                    state_code="US_XX",
                    external_id=f"external_id_{response_date.isoformat()}",
                    sequence_num=i,
                    response_date=response_date,
                )
            )

        return violation_response_utils.responses_on_most_recent_response_date(
            violation_responses
        )

    def test_responses_on_most_recent_response_date(self) -> None:
        response_dates = [
            datetime.date(2020, 1, 1),
            datetime.date(2019, 3, 1),
            datetime.date(2024, 10, 1),
        ]

        most_recent_responses = self._test_responses_on_most_recent_response_date(
            response_dates
        )

        self.assertEqual(1, len(most_recent_responses))
        self.assertEqual(
            datetime.date(2024, 10, 1), most_recent_responses[0].response_date
        )

    def test_responses_on_most_recent_response_date_multiple_on_most_recent(
        self,
    ) -> None:
        response_dates = [
            datetime.date(2020, 1, 1),
            datetime.date(2024, 10, 1),
            datetime.date(2024, 10, 1),
        ]

        most_recent_responses = self._test_responses_on_most_recent_response_date(
            response_dates
        )

        self.assertEqual(2, len(most_recent_responses))

        for response in most_recent_responses:
            self.assertEqual(datetime.date(2024, 10, 1), response.response_date)


class TestDefaultFilteredViolationResponsesForViolationHistory(unittest.TestCase):
    """Tests the filter_violation_responses_for_violation_history function."""

    def setUp(self) -> None:
        self.delegate = UsXxViolationDelegate()

    def test_filter_violation_responses_for_violation_history(self) -> None:
        violation_responses = [
            NormalizedStateSupervisionViolationResponse(
                supervision_violation_response_id=1,
                state_code="US_XX",
                external_id="svr1",
                response_type=StateSupervisionViolationResponseType.PERMANENT_DECISION,
                response_date=datetime.date(2000, 1, 1),
                sequence_num=0,
            ),
            NormalizedStateSupervisionViolationResponse(
                supervision_violation_response_id=1,
                state_code="US_XX",
                external_id="svr2",
                response_type=StateSupervisionViolationResponseType.VIOLATION_REPORT,
                response_date=datetime.date(1998, 2, 1),
                sequence_num=1,
            ),
            NormalizedStateSupervisionViolationResponse(
                supervision_violation_response_id=1,
                state_code="US_XX",
                external_id="svr3",
                response_type=StateSupervisionViolationResponseType.VIOLATION_REPORT,
                response_date=datetime.date(1997, 3, 1),
                sequence_num=2,
            ),
        ]

        filtered_responses = filter_violation_responses_for_violation_history(
            violation_delegate=self.delegate,
            violation_responses=violation_responses,
            include_follow_up_responses=False,
        )

        self.assertCountEqual(
            [
                NormalizedStateSupervisionViolationResponse(
                    supervision_violation_response_id=1,
                    state_code="US_XX",
                    external_id="svr2",
                    response_type=StateSupervisionViolationResponseType.VIOLATION_REPORT,
                    response_date=datetime.date(1998, 2, 1),
                    sequence_num=1,
                ),
                NormalizedStateSupervisionViolationResponse(
                    supervision_violation_response_id=1,
                    state_code="US_XX",
                    external_id="svr3",
                    response_type=StateSupervisionViolationResponseType.VIOLATION_REPORT,
                    response_date=datetime.date(1997, 3, 1),
                    sequence_num=2,
                ),
            ],
            filtered_responses,
        )

    def test_default_filtered_violation_responses_for_violation_history_empty(
        self,
    ) -> None:
        violation_responses: List[NormalizedStateSupervisionViolationResponse] = []

        filtered_responses = filter_violation_responses_for_violation_history(
            violation_delegate=self.delegate,
            violation_responses=violation_responses,
            include_follow_up_responses=False,
        )

        self.assertEqual(
            [],
            filtered_responses,
        )


class TestViolationResponsesInWindow(unittest.TestCase):
    """Test the violation_responses_in_window function."""

    def test_violation_responses_in_window(self) -> None:
        violation_responses = [
            NormalizedStateSupervisionViolationResponse(
                supervision_violation_response_id=1,
                state_code="US_XX",
                external_id="svr1",
                response_type=StateSupervisionViolationResponseType.VIOLATION_REPORT,
                response_date=datetime.date(2010, 1, 1),
                sequence_num=0,
            ),
            NormalizedStateSupervisionViolationResponse(
                supervision_violation_response_id=1,
                state_code="US_XX",
                external_id="svr2",
                response_type=StateSupervisionViolationResponseType.VIOLATION_REPORT,
                response_date=datetime.date(1998, 2, 1),
                sequence_num=1,
            ),
            NormalizedStateSupervisionViolationResponse(
                supervision_violation_response_id=1,
                state_code="US_XX",
                external_id="svr3",
                response_type=StateSupervisionViolationResponseType.VIOLATION_REPORT,
                response_date=datetime.date(2017, 3, 1),
                sequence_num=2,
            ),
        ]

        lower_bound_inclusive = datetime.date(2009, 1, 17)
        upper_bound_exclusive = datetime.date(2010, 1, 18)

        responses_in_window = violation_responses_in_window(
            violation_responses,
            upper_bound_exclusive=upper_bound_exclusive,
            lower_bound_inclusive=lower_bound_inclusive,
        )

        self.assertEqual(
            [
                NormalizedStateSupervisionViolationResponse(
                    supervision_violation_response_id=1,
                    state_code="US_XX",
                    external_id="svr1",
                    response_type=StateSupervisionViolationResponseType.VIOLATION_REPORT,
                    response_date=datetime.date(2010, 1, 1),
                    sequence_num=0,
                )
            ],
            responses_in_window,
        )

    def test_violation_responses_in_window_no_lower_bound(self) -> None:
        violation_responses = [
            NormalizedStateSupervisionViolationResponse(
                supervision_violation_response_id=1,
                state_code="US_XX",
                external_id="svr1",
                response_type=StateSupervisionViolationResponseType.VIOLATION_REPORT,
                response_date=datetime.date(2010, 1, 1),
                sequence_num=0,
            ),
            NormalizedStateSupervisionViolationResponse(
                supervision_violation_response_id=1,
                state_code="US_XX",
                external_id="svr2",
                response_type=StateSupervisionViolationResponseType.VIOLATION_REPORT,
                response_date=datetime.date(1990, 2, 1),
                sequence_num=1,
            ),
            NormalizedStateSupervisionViolationResponse(
                supervision_violation_response_id=1,
                state_code="US_XX",
                external_id="svr3",
                response_type=StateSupervisionViolationResponseType.VIOLATION_REPORT,
                response_date=datetime.date(2017, 3, 1),
                sequence_num=2,
            ),
        ]

        upper_bound_exclusive = datetime.date(2010, 1, 17)

        responses_in_window = violation_responses_in_window(
            violation_responses,
            upper_bound_exclusive=upper_bound_exclusive,
            lower_bound_inclusive=None,
        )

        self.assertCountEqual(
            [
                NormalizedStateSupervisionViolationResponse(
                    supervision_violation_response_id=1,
                    state_code="US_XX",
                    external_id="svr2",
                    response_type=StateSupervisionViolationResponseType.VIOLATION_REPORT,
                    response_date=datetime.date(1990, 2, 1),
                    sequence_num=1,
                ),
                NormalizedStateSupervisionViolationResponse(
                    supervision_violation_response_id=1,
                    state_code="US_XX",
                    external_id="svr1",
                    response_type=StateSupervisionViolationResponseType.VIOLATION_REPORT,
                    response_date=datetime.date(2010, 1, 1),
                    sequence_num=0,
                ),
            ],
            responses_in_window,
        )

    def test_violation_responses_in_window_all_outside_of_window(self) -> None:
        violation_responses = [
            NormalizedStateSupervisionViolationResponse(
                supervision_violation_response_id=1,
                state_code="US_XX",
                external_id="svr1",
                response_type=StateSupervisionViolationResponseType.VIOLATION_REPORT,
                response_date=datetime.date(2000, 1, 1),
                sequence_num=0,
            ),
            NormalizedStateSupervisionViolationResponse(
                supervision_violation_response_id=1,
                state_code="US_XX",
                external_id="svr2",
                response_type=StateSupervisionViolationResponseType.VIOLATION_REPORT,
                response_date=datetime.date(1998, 2, 1),
                sequence_num=1,
            ),
            NormalizedStateSupervisionViolationResponse(
                supervision_violation_response_id=1,
                state_code="US_XX",
                external_id="svr3",
                response_type=StateSupervisionViolationResponseType.VIOLATION_REPORT,
                response_date=datetime.date(2019, 3, 1),
                sequence_num=2,
            ),
        ]

        lower_bound_inclusive = datetime.date(2009, 1, 17)
        upper_bound_exclusive = datetime.date(2010, 1, 18)

        responses_in_window = violation_responses_in_window(
            violation_responses,
            upper_bound_exclusive=upper_bound_exclusive,
            lower_bound_inclusive=lower_bound_inclusive,
        )

        self.assertEqual([], responses_in_window)

    def test_violation_responses_in_window_exclude_before_window(self) -> None:
        violation_responses = [
            NormalizedStateSupervisionViolationResponse(
                supervision_violation_response_id=1,
                state_code="US_XX",
                external_id="svr1",
                response_type=StateSupervisionViolationResponseType.VIOLATION_REPORT,
                response_date=datetime.date(2000, 1, 1),
                sequence_num=0,
            ),
            NormalizedStateSupervisionViolationResponse(
                supervision_violation_response_id=1,
                state_code="US_XX",
                external_id="svr2",
                response_type=StateSupervisionViolationResponseType.VIOLATION_REPORT,
                response_date=datetime.date(1998, 2, 1),
                sequence_num=1,
            ),
            NormalizedStateSupervisionViolationResponse(
                supervision_violation_response_id=1,
                state_code="US_XX",
                external_id="svr3",
                response_type=StateSupervisionViolationResponseType.VIOLATION_REPORT,
                response_date=datetime.date(1997, 3, 1),
                sequence_num=2,
            ),
        ]

        lower_bound_inclusive = datetime.date(2009, 1, 17)
        upper_bound_exclusive = datetime.date(2010, 1, 18)

        responses_in_window = violation_responses_in_window(
            violation_responses,
            upper_bound_exclusive=upper_bound_exclusive,
            lower_bound_inclusive=lower_bound_inclusive,
        )

        self.assertEqual([], responses_in_window)


class TestIdentifyMostSevereResponseDecision(unittest.TestCase):
    """Tests the identify_most_severe_response_decision function."""

    def test_decision_severity_order_comprehension(self) -> None:
        for decision in StateSupervisionViolationResponseDecision:
            self.assertIn(decision, DECISION_SEVERITY_ORDER)

    def test_identify_most_severe_response_decision(self) -> None:
        decisions = [
            StateSupervisionViolationResponseDecision.CONTINUANCE,
            StateSupervisionViolationResponseDecision.REVOCATION,
        ]

        most_severe_decision = identify_most_severe_response_decision(decisions)

        self.assertEqual(
            most_severe_decision, StateSupervisionViolationResponseDecision.REVOCATION
        )

    def test_identify_most_severe_response_decision_test_all_types(self) -> None:
        for decision in StateSupervisionViolationResponseDecision:
            decisions = [decision]

            most_severe_decision = identify_most_severe_response_decision(decisions)

            self.assertEqual(most_severe_decision, decision)
