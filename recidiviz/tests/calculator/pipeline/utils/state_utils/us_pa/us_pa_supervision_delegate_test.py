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
from datetime import date
from typing import Optional

from parameterized import parameterized

from recidiviz.calculator.pipeline.utils.state_utils.us_pa.us_pa_supervision_delegate import (
    UsPaSupervisionDelegate,
)
from recidiviz.common.constants.state.state_assessment import StateAssessmentLevel
from recidiviz.common.constants.state.state_sentence import StateSentenceStatus
from recidiviz.common.constants.state.state_supervision_period import (
    StateSupervisionLevel,
    StateSupervisionPeriodSupervisionType,
    StateSupervisionPeriodTerminationReason,
)
from recidiviz.persistence.entity.state.entities import (
    StateIncarcerationSentence,
    StateSupervisionPeriod,
)


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

    def test_get_projected_completion_date_supervision_period_in_incarceration_sentence(
        self,
    ) -> None:
        supervision_period = StateSupervisionPeriod.new_with_defaults(
            supervision_period_id=111,
            state_code="US_PA",
            start_date=date(2018, 5, 1),
            termination_date=date(2018, 5, 15),
            termination_reason=StateSupervisionPeriodTerminationReason.DISCHARGE,
            supervision_type=StateSupervisionPeriodSupervisionType.PROBATION,
            supervision_level=StateSupervisionLevel.MEDIUM,
            supervision_level_raw_text="M",
        )

        incarceration_sentence = StateIncarcerationSentence.new_with_defaults(
            state_code="US_PA",
            incarceration_sentence_id=123,
            start_date=date(2018, 5, 1),
            supervision_periods=[supervision_period],
            projected_max_release_date=date(2018, 5, 10),
            status=StateSentenceStatus.PRESENT_WITHOUT_INFO,
        )

        self.assertEqual(
            self.supervision_delegate.get_projected_completion_date(
                supervision_period=supervision_period,
                incarceration_sentences=[incarceration_sentence],
                supervision_sentences=[],
            ),
            incarceration_sentence.projected_max_release_date,
        )

    def test_get_projected_completion_date_supervision_period_no_sentence_overlapping(
        self,
    ) -> None:
        supervision_period = StateSupervisionPeriod.new_with_defaults(
            supervision_period_id=111,
            state_code="US_PA",
            start_date=date(2018, 5, 1),
            termination_date=date(2018, 5, 15),
            termination_reason=StateSupervisionPeriodTerminationReason.DISCHARGE,
            supervision_type=StateSupervisionPeriodSupervisionType.PROBATION,
            supervision_level=StateSupervisionLevel.MEDIUM,
            supervision_level_raw_text="M",
        )

        incarceration_sentence = StateIncarcerationSentence.new_with_defaults(
            state_code="US_PA",
            incarceration_sentence_id=123,
            start_date=date(2018, 4, 1),
            completion_date=date(2018, 4, 30),
            supervision_periods=[],
            projected_max_release_date=date(2018, 5, 10),
            status=StateSentenceStatus.PRESENT_WITHOUT_INFO,
        )

        self.assertIsNone(
            self.supervision_delegate.get_projected_completion_date(
                supervision_period=supervision_period,
                incarceration_sentences=[incarceration_sentence],
                supervision_sentences=[],
            )
        )

    def test_get_projected_completion_date_supervision_period_multiple_sentences(
        self,
    ) -> None:
        """Tests that if there are multiple overlapping sentences, we take only the
        latest date."""
        supervision_period = StateSupervisionPeriod.new_with_defaults(
            supervision_period_id=111,
            state_code="US_PA",
            start_date=date(2018, 5, 1),
            termination_date=date(2018, 5, 15),
            termination_reason=StateSupervisionPeriodTerminationReason.DISCHARGE,
            supervision_type=StateSupervisionPeriodSupervisionType.PROBATION,
            supervision_level=StateSupervisionLevel.MEDIUM,
            supervision_level_raw_text="M",
        )

        incarceration_sentence = StateIncarcerationSentence.new_with_defaults(
            state_code="US_PA",
            incarceration_sentence_id=123,
            start_date=date(2018, 5, 1),
            supervision_periods=[supervision_period],
            projected_max_release_date=date(2018, 5, 10),
            status=StateSentenceStatus.PRESENT_WITHOUT_INFO,
        )
        incarceration_sentence_2 = StateIncarcerationSentence.new_with_defaults(
            state_code="US_PA",
            incarceration_sentence_id=123,
            start_date=date(2018, 5, 1),
            supervision_periods=[supervision_period],
            projected_max_release_date=date(2018, 6, 10),
            status=StateSentenceStatus.PRESENT_WITHOUT_INFO,
        )

        self.assertEqual(
            self.supervision_delegate.get_projected_completion_date(
                supervision_period=supervision_period,
                incarceration_sentences=[
                    incarceration_sentence,
                    incarceration_sentence_2,
                ],
                supervision_sentences=[],
            ),
            incarceration_sentence_2.projected_max_release_date,
        )
