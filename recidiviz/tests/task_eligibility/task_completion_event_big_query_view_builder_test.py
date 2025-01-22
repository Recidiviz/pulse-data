# Recidiviz - a data platform for criminal justice reform
# Copyright (C) 2025 Recidiviz, Inc.
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
"""Tests for the TaskCompletionEventBigQueryViewBuilder."""
import unittest
from typing import List

from recidiviz.task_eligibility.task_completion_event_big_query_view_builder import (
    StateAgnosticTaskCompletionEventBigQueryViewBuilder,
    StateSpecificTaskCompletionEventBigQueryViewBuilder,
)
from recidiviz.task_eligibility.task_completion_event_big_query_view_collector import (
    TaskCompletionEventBigQueryViewCollector,
)


class TestStateAgnosticEventsHaveCorrectExclusions(unittest.TestCase):
    """Tests that state agnostic completion event view builders have the correct states excluded based on there being
    a state specific completion event of the same type"""

    state_specific_builder_list: List[
        StateSpecificTaskCompletionEventBigQueryViewBuilder
    ]
    state_agnostic_builder_list: List[
        StateAgnosticTaskCompletionEventBigQueryViewBuilder
    ]

    @classmethod
    def setUpClass(cls) -> None:
        """Create separate lists for state-specific and state-agnostic completion event view builders"""
        cls.state_specific_builder_list = []
        cls.state_agnostic_builder_list = []
        for (
            builder
        ) in TaskCompletionEventBigQueryViewCollector().collect_view_builders():
            if isinstance(builder, StateSpecificTaskCompletionEventBigQueryViewBuilder):
                cls.state_specific_builder_list.append(builder)
            elif isinstance(
                builder, StateAgnosticTaskCompletionEventBigQueryViewBuilder
            ):
                cls.state_agnostic_builder_list.append(builder)

    def test_that_state_agnostic_events_have_correct_state_exclusions(self) -> None:
        self.maxDiff = None

        state_agnostic_exclusions_actual = {}
        state_agnostic_exclusions_expected = {}

        for state_agnostic_builder in self.state_agnostic_builder_list:
            state_list_actual = (
                []
                if not state_agnostic_builder.states_to_exclude
                else [x.name for x in state_agnostic_builder.states_to_exclude]
            )
            # create the expected state list by looping through each state-specific builder and appending a list of the
            # states that have that builder view id
            state_list_expected = []
            for state_specific_builder in self.state_specific_builder_list:
                if state_agnostic_builder.view_id == state_specific_builder.view_id:
                    state_list_expected.append(state_specific_builder.state_code.name)
            state_agnostic_exclusions_actual[state_agnostic_builder.view_id] = set(
                state_list_actual
            )
            state_agnostic_exclusions_expected[state_agnostic_builder.view_id] = set(
                state_list_expected
            )

        self.assertEqual(
            state_agnostic_exclusions_expected,
            state_agnostic_exclusions_actual,
            msg="Incorrect state exclusion made on state agnostic completion event",
        )
