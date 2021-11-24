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
"""Tests for the us_pa_supervision_delegate.py file"""
import unittest
from typing import Optional

from parameterized import parameterized

from recidiviz.calculator.pipeline.utils.state_utils.us_pa.us_pa_supervision_delegate import (
    UsPaSupervisionDelegate,
)
from recidiviz.common.constants.state.state_assessment import StateAssessmentLevel


class TestUsPaSupervisionDelegate(unittest.TestCase):
    """Unit tests for TestUsPaSupervisionDelegate"""

    def setUp(self) -> None:
        self.supervision_delegate = UsPaSupervisionDelegate()

    @parameterized.expand(
        [
            ("CO|CO - CENTRAL OFFICE|9110", "CO - CENTRAL OFFICE", "CO"),
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

    def test_lsir_score_bucket(self) -> None:
        self.assertEqual(
            self.supervision_delegate.set_lsir_assessment_score_bucket(
                assessment_score=11,
                assessment_level=StateAssessmentLevel.LOW,
            ),
            StateAssessmentLevel.LOW.value,
        )

    def test_lsir_score_bucket_no_level(self) -> None:
        self.assertIsNone(
            self.supervision_delegate.set_lsir_assessment_score_bucket(
                assessment_score=11,
                assessment_level=None,
            )
        )
