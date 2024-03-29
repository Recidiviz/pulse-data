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
"""Tests for classes/untils in recidiviz/common/constants/state/state_supervision_period.py."""

import unittest

from recidiviz.common.constants.state.state_supervision_period import (
    StateSupervisionLevel,
    StateSupervisionPeriodSupervisionType,
    get_most_relevant_supervision_type,
)


class StateSupervisionPeriodTest(unittest.TestCase):
    """Tests for classes/untils in recidiviz/common/constants/state/state_supervision_period.py."""

    def test_getMostRelevantSupervisionType_allEnums(self) -> None:
        for supervision_type in StateSupervisionPeriodSupervisionType:
            types = {supervision_type}
            self.assertEqual(
                supervision_type, get_most_relevant_supervision_type(types)
            )

    def test_getMostRelevantSupervisionType_chooseDualIfExists(self) -> None:
        for supervision_type in StateSupervisionPeriodSupervisionType:
            types = {StateSupervisionPeriodSupervisionType.DUAL, supervision_type}
            self.assertEqual(
                StateSupervisionPeriodSupervisionType.DUAL,
                get_most_relevant_supervision_type(types),
            )

    def test_getMostRelevantSupervisionType_dualIfProbationAndParole(self) -> None:
        types = {
            StateSupervisionPeriodSupervisionType.PROBATION,
            StateSupervisionPeriodSupervisionType.PAROLE,
        }
        self.assertEqual(
            StateSupervisionPeriodSupervisionType.DUAL,
            get_most_relevant_supervision_type(types),
        )


class StateSupervisionLevelTest(unittest.TestCase):
    """Tests comparators for StateSupervisionLevel."""

    def test_non_comparable(self) -> None:
        with self.assertRaises(NotImplementedError):
            if StateSupervisionLevel.EXTERNAL_UNKNOWN < StateSupervisionLevel.MINIMUM:
                pass

        with self.assertRaises(NotImplementedError):
            if StateSupervisionLevel.MEDIUM < StateSupervisionLevel.INTERNAL_UNKNOWN:
                pass

    def test_pairwise_comparisons(self) -> None:
        rankings = StateSupervisionLevel.get_comparable_level_rankings()
        for level_1, rank_1 in rankings.items():
            for level_2, rank_2 in rankings.items():
                self.assertEqual(level_1 < level_2, rank_1 < rank_2)
                self.assertEqual(level_1 > level_2, rank_1 > rank_2)
                self.assertEqual(level_1 >= level_2, rank_1 >= rank_2)
                self.assertEqual(level_1 <= level_2, rank_1 <= rank_2)
