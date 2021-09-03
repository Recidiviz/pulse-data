# Recidiviz - a data platform for criminal justice reform
# Copyright (C) 2020 Recidiviz, Inc.
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
"""Tests the functions in the event_utils file."""
import unittest
from datetime import date

from recidiviz.calculator.pipeline.program.events import ProgramReferralEvent
from recidiviz.calculator.pipeline.utils.event_utils import AssessmentEventMixin
from recidiviz.common.constants.state.state_assessment import (
    StateAssessmentLevel,
    StateAssessmentType,
)


class TestAssessmentEventMixin(unittest.TestCase):
    """Tests the functionality of the AssessmentEventMixin class."""

    def setUp(self) -> None:
        self.lsir_scores_to_buckets = {19: "0-23", 27: "24-29", 30: "30-38", 39: "39+"}

    def test_assessment_scores_all_types(self):
        for assessment_type in StateAssessmentType:
            event = ProgramReferralEvent(
                state_code="US_XX",
                event_date=date.today(),
                program_id="xxx",
                assessment_score=10,
                assessment_type=assessment_type,
            )

            # Assert this doesn't fail for all assessment types
            _ = event.assessment_score_bucket

    def test_assessment_scores_to_buckets_LSIR(self):
        for score, bucket in self.lsir_scores_to_buckets.items():
            event = ProgramReferralEvent(
                state_code="US_XX",
                event_date=date.today(),
                program_id="xxx",
                assessment_score=score,
                assessment_type=StateAssessmentType.LSIR,
            )

            self.assertEqual(bucket, event.assessment_score_bucket)

    def test_assessment_scores_to_buckets_LSIR_no_score(self):
        event = ProgramReferralEvent(
            state_code="US_XX",
            event_date=date.today(),
            program_id="xxx",
            assessment_score=None,
            assessment_type=StateAssessmentType.LSIR,
        )

        self.assertEqual(
            AssessmentEventMixin.DEFAULT_ASSESSMENT_SCORE_BUCKET,
            event.assessment_score_bucket,
        )

    def test_assessment_scores_to_buckets_LSIR_US_PA(self):
        event = ProgramReferralEvent(
            state_code="US_PA",
            event_date=date.today(),
            program_id="xxx",
            assessment_score=11,
            assessment_type=StateAssessmentType.LSIR,
            assessment_level=StateAssessmentLevel.LOW,
        )

        self.assertEqual(StateAssessmentLevel.LOW.value, event.assessment_score_bucket)

    def test_assessment_scores_to_buckets_LSIR_US_PA_no_level(self):
        event = ProgramReferralEvent(
            state_code="US_PA",
            event_date=date.today(),
            program_id="xxx",
            assessment_score=11,
            assessment_type=StateAssessmentType.LSIR,
            assessment_level=None,
        )

        self.assertEqual(
            AssessmentEventMixin.DEFAULT_ASSESSMENT_SCORE_BUCKET,
            event.assessment_score_bucket,
        )

    def test_assessment_score_bucket_ORAS(self):
        event = ProgramReferralEvent(
            state_code="US_XX",
            event_date=date.today(),
            program_id="xxx",
            assessment_score=10,
            assessment_level=StateAssessmentLevel.MEDIUM,
            assessment_type=StateAssessmentType.ORAS_COMMUNITY_SUPERVISION,
        )

        self.assertEqual(
            StateAssessmentLevel.MEDIUM.value, event.assessment_score_bucket
        )

    def test_assessment_score_bucket_ORAS_no_level(self):
        event = ProgramReferralEvent(
            state_code="US_XX",
            event_date=date.today(),
            program_id="xxx",
            assessment_score=10,
            assessment_level=None,
            assessment_type=StateAssessmentType.ORAS_COMMUNITY_SUPERVISION,
        )

        self.assertEqual(
            AssessmentEventMixin.DEFAULT_ASSESSMENT_SCORE_BUCKET,
            event.assessment_score_bucket,
        )

    def test_assessment_score_bucket_unsupported(self):
        event = ProgramReferralEvent(
            state_code="US_XX",
            event_date=date.today(),
            program_id="xxx",
            assessment_score=10,
            assessment_level=StateAssessmentLevel.MEDIUM,
            assessment_type=StateAssessmentType.PSA,
        )

        self.assertEqual(
            AssessmentEventMixin.DEFAULT_ASSESSMENT_SCORE_BUCKET,
            event.assessment_score_bucket,
        )

    def test_assessment_score_no_type(self):
        event = ProgramReferralEvent(
            state_code="US_XX",
            event_date=date.today(),
            program_id="xxx",
            assessment_score=10,
            assessment_type=None,
        )

        self.assertEqual(
            AssessmentEventMixin.DEFAULT_ASSESSMENT_SCORE_BUCKET,
            event.assessment_score_bucket,
        )
