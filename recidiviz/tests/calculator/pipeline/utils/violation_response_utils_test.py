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

import pytest

from recidiviz.calculator.pipeline.utils import violation_response_utils
from recidiviz.persistence.entity.state.entities import (
    StateSupervisionViolationResponse,
)


class TestResponsesOnMostRecentResponseDate(unittest.TestCase):
    """Tests the responses_on_most_recent_response_date function."""

    @staticmethod
    def _test_responses_on_most_recent_response_date(
        response_dates: List[datetime.date],
    ) -> List[StateSupervisionViolationResponse]:
        """Helper function for testing the responses_on_most_recent_response_date function."""
        violation_responses: List[StateSupervisionViolationResponse] = []

        for response_date in response_dates:
            violation_responses.append(
                StateSupervisionViolationResponse.new_with_defaults(
                    state_code="US_XX", response_date=response_date
                )
            )

        return violation_response_utils.responses_on_most_recent_response_date(
            violation_responses
        )

    def test_responses_on_most_recent_response_date(self):
        response_dates = [
            datetime.date(2020, 1, 1),
            datetime.date(2019, 3, 1),
            datetime.date(2029, 10, 1),
        ]

        most_recent_responses = self._test_responses_on_most_recent_response_date(
            response_dates
        )

        self.assertEqual(1, len(most_recent_responses))
        self.assertEqual(
            datetime.date(2029, 10, 1), most_recent_responses[0].response_date
        )

    def test_responses_on_most_recent_response_date_empty_dates(self):
        response_dates = [
            datetime.date(2020, 1, 1),
            datetime.date(2019, 3, 1),
            datetime.date(2029, 10, 1),
            None,
        ]

        # No responses without dates should be sent to this function
        with pytest.raises(ValueError):
            _ = self._test_responses_on_most_recent_response_date(response_dates)

    def test_responses_on_most_recent_response_date_multiple_on_most_recent(self):
        response_dates = [
            datetime.date(2020, 1, 1),
            datetime.date(2029, 10, 1),
            datetime.date(2029, 10, 1),
        ]

        most_recent_responses = self._test_responses_on_most_recent_response_date(
            response_dates
        )

        self.assertEqual(2, len(most_recent_responses))

        for response in most_recent_responses:
            self.assertEqual(datetime.date(2029, 10, 1), response.response_date)
