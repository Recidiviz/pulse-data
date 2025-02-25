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
"""Tests the generated SQL made by vitals_view_helpers.py."""

from unittest import TestCase

from recidiviz.calculator.query.state.views.dashboard.vitals_summaries.vitals_view_helpers import (
    make_enabled_states_filter,
    make_enabled_states_filter_for_vital,
)

MOCK_ENABLED_VITALS = {
    "US_XX": ["metric1", "metric2"],
    "US_YY": ["metric2", "metric6", "metric4"],
}


class VitalsViewHelpersTest(TestCase):
    """Tests for state/views/vitals_report/vitals_view_helpers.py."""

    def test_enabled_states_filter(self) -> None:
        states_filter = make_enabled_states_filter(MOCK_ENABLED_VITALS)

        self.assertEqual(states_filter, "state_code in ('US_XX', 'US_YY')")

    def test_enabled_states_filter_for_vital_single_state(self) -> None:
        states_filter = make_enabled_states_filter_for_vital(
            "metric1", MOCK_ENABLED_VITALS
        )

        self.assertEqual(states_filter, "state_code in ('US_XX')")

    def test_enabled_states_filter_for_vital_multiple_states(self) -> None:
        states_filter = make_enabled_states_filter_for_vital(
            "metric2", MOCK_ENABLED_VITALS
        )

        self.assertEqual(states_filter, "state_code in ('US_XX', 'US_YY')")
