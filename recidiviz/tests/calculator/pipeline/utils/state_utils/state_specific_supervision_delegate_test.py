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
"""Unit tests for state_specific_supervision_delegate default functions"""
import unittest

from recidiviz.tests.calculator.pipeline.utils.state_utils.us_xx.us_xx_supervision_delegate import (
    UsXxSupervisionDelegate,
)


class TestStateSpecificSupervisionDelegate(unittest.TestCase):
    """Unit tests for state_specific_supervision_delegate default function implementations."""

    def setUp(self) -> None:
        self.supervision_delegate = UsXxSupervisionDelegate()

    def test_supervision_location_from_supervision_site(self) -> None:
        (
            level_1,
            level_2,
        ) = self.supervision_delegate.supervision_location_from_supervision_site("1")
        self.assertEqual(level_1, "1")
        self.assertEqual(level_2, None)
