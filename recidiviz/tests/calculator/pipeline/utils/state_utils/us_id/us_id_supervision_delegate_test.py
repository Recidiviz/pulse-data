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
"""Tests for the us_id_supervision_delegate.py file"""
import unittest
from typing import Optional

from parameterized import parameterized

from recidiviz.calculator.pipeline.utils.state_utils.us_id.us_id_supervision_delegate import (
    UsIdSupervisionDelegate,
)


class TestUsIdSupervisionDelegate(unittest.TestCase):
    """Unit tests for TestUsIdSupervisionDelegate"""

    def setUp(self) -> None:
        self.supervision_delegate = UsIdSupervisionDelegate()

    @parameterized.expand(
        [
            (
                "DISTRICT OFFICE 6, POCATELLO|UNKNOWN",
                "UNKNOWN",
                "DISTRICT OFFICE 6, POCATELLO",
            ),
            (
                "PAROLE COMMISSION OFFICE|DEPORTED",
                "DEPORTED",
                "PAROLE COMMISSION OFFICE",
            ),
            ("DISTRICT OFFICE 4, BOISE|", None, "DISTRICT OFFICE 4, BOISE"),
            (None, None, None),
        ]
    )
    def test_supervision_location_from_supervision_site(
        self,
        supervision_site: Optional[str],
        expected_level_1_supervision_location: Optional[str],
        expected_level_2_supervision_location: Optional[str],
    ) -> None:
        (
            level_1_supervision_location,
            level_2_supervision_location,
        ) = self.supervision_delegate.supervision_location_from_supervision_site(
            supervision_site
        )
        self.assertEqual(
            level_1_supervision_location, expected_level_1_supervision_location
        )
        self.assertEqual(
            level_2_supervision_location, expected_level_2_supervision_location
        )
