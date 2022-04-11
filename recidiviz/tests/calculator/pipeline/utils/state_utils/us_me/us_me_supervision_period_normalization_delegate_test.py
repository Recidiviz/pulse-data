#  Recidiviz - a data platform for criminal justice reform
#  Copyright (C) 2022 Recidiviz, Inc.
#
#  This program is free software: you can redistribute it and/or modify
#  it under the terms of the GNU General Public License as published by
#  the Free Software Foundation, either version 3 of the License, or
#  (at your option) any later version.
#
#  This program is distributed in the hope that it will be useful,
#  but WITHOUT ANY WARRANTY; without even the implied warranty of
#  MERCHANTABILITY or FITNESS FOR A PARTICULAR PURPOSE.  See the
#  GNU General Public License for more details.
#
#  You should have received a copy of the GNU General Public License
#  along with this program.  If not, see <https://www.gnu.org/licenses/>.
#  =============================================================================
"""Tests us_me_supervision_period_normalization_delegate.py."""
import unittest
from datetime import date
from typing import List

from recidiviz.calculator.pipeline.utils.state_utils.us_me.us_me_supervision_period_normalization_delegate import (
    UsMeSupervisionNormalizationDelegate,
)
from recidiviz.common.constants.state.state_assessment import (
    StateAssessmentLevel,
    StateAssessmentType,
)
from recidiviz.common.constants.state.state_sentence import StateSentenceStatus
from recidiviz.common.constants.state.state_supervision_period import (
    StateSupervisionLevel,
    StateSupervisionPeriodTerminationReason,
)
from recidiviz.common.constants.states import StateCode
from recidiviz.persistence.entity.state.entities import (
    StateAssessment,
    StateSupervisionPeriod,
    StateSupervisionSentence,
)

_STATE_CODE = StateCode.US_ME.value


class TestUsMeSupervisionNormalizationDelegate(unittest.TestCase):
    """Tests functions in UsMeSupervisionNormalizationDelegate."""

    @staticmethod
    def _build_delegate(
        assessments: List[StateAssessment],
    ) -> UsMeSupervisionNormalizationDelegate:
        return UsMeSupervisionNormalizationDelegate(assessments=assessments)

    def test_supervision_level_override_active_period(self) -> None:
        """Assert that assessments taken within an active period are used to set the supervision level."""
        supervision_period = StateSupervisionPeriod.new_with_defaults(
            state_code=StateCode.US_ME.value,
            start_date=date(2010, 1, 1),
            termination_date=None,
        )
        assessments = [
            StateAssessment.new_with_defaults(
                state_code=StateCode.US_ME.value,
                assessment_date=date(2010, 1, 1),
                assessment_type_raw_text="ADULT, FEMALE, COMMUNITY",
                assessment_level=StateAssessmentLevel.MODERATE,
            )
        ]
        self.assertEqual(
            StateSupervisionLevel.MEDIUM,
            self._build_delegate(assessments).supervision_level_override(
                supervision_period
            ),
        )

    def test_supervision_level_override_no_assessments(self) -> None:
        """Assert that with no assessments within the supervision period, supervision level is set to
        internal unknown."""
        supervision_period = StateSupervisionPeriod.new_with_defaults(
            state_code=StateCode.US_ME.value,
            start_date=date(2010, 1, 1),
            termination_date=date(2010, 3, 30),
        )
        assessments: List[StateAssessment] = []
        self.assertEqual(
            StateSupervisionLevel.INTERNAL_UNKNOWN,
            self._build_delegate(assessments).supervision_level_override(
                supervision_period
            ),
        )

    def test_supervision_level_override_spin_w_most_recent(self) -> None:
        """Assert that spin-w is used if it is the most recent."""
        supervision_period = StateSupervisionPeriod.new_with_defaults(
            state_code=StateCode.US_ME.value,
            start_date=date(2010, 1, 1),
            termination_date=date(2010, 3, 30),
        )
        assessments = [
            StateAssessment.new_with_defaults(
                state_code=StateCode.US_ME.value,
                assessment_date=date(2010, 3, 1),
                assessment_type_raw_text="SPIN-W",
                assessment_level=StateAssessmentLevel.VERY_HIGH,
            ),
            StateAssessment.new_with_defaults(
                state_code=StateCode.US_ME.value,
                assessment_date=date(2010, 1, 1),
                assessment_type_raw_text="ADULT, FEMALE, COMMUNITY",
                assessment_level=StateAssessmentLevel.MODERATE,
            ),
        ]
        self.assertEqual(
            StateSupervisionLevel.HIGH,
            self._build_delegate(assessments).supervision_level_override(
                supervision_period
            ),
        )

    def test_supervision_level_override_lsir_most_recent(self) -> None:
        """Assert that LSIR Adult, Female, Community is used if it is the most recent."""
        supervision_period = StateSupervisionPeriod.new_with_defaults(
            state_code=StateCode.US_ME.value,
            start_date=date(2010, 1, 1),
            termination_date=date(2010, 3, 30),
        )
        assessments = [
            StateAssessment.new_with_defaults(
                state_code=StateCode.US_ME.value,
                assessment_date=date(2010, 3, 1),
                assessment_type_raw_text="SPIN-W",
                assessment_level=StateAssessmentLevel.VERY_HIGH,
            ),
            StateAssessment.new_with_defaults(
                state_code=StateCode.US_ME.value,
                assessment_date=date(2010, 1, 1),
                assessment_type_raw_text="ADULT, FEMALE, COMMUNITY",
                assessment_level=StateAssessmentLevel.MODERATE,
            ),
            StateAssessment.new_with_defaults(
                state_code=StateCode.US_ME.value,
                assessment_date=date(2010, 3, 15),
                assessment_type_raw_text="ADULT, FEMALE, COMMUNITY",
                assessment_level=StateAssessmentLevel.MAXIMUM,
            ),
        ]
        self.assertEqual(
            StateSupervisionLevel.MAXIMUM,
            self._build_delegate(assessments).supervision_level_override(
                supervision_period
            ),
        )

    def test_supervision_level_override_most_recent_static_99(self) -> None:
        """Assert that the most recent Static 99 assessment level is used."""
        supervision_period = StateSupervisionPeriod.new_with_defaults(
            state_code=StateCode.US_ME.value,
            start_date=date(2010, 1, 1),
            termination_date=date(2010, 3, 30),
        )
        assessments = [
            StateAssessment.new_with_defaults(
                state_code=StateCode.US_ME.value,
                assessment_date=date(2010, 3, 1),
                assessment_type=StateAssessmentType.STATIC_99,
                assessment_type_raw_text="STATIC 99",
                assessment_level=StateAssessmentLevel.MODERATE,
            ),
            StateAssessment.new_with_defaults(
                state_code=StateCode.US_ME.value,
                assessment_date=date(2010, 3, 15),
                assessment_type=StateAssessmentType.LSIR,
                assessment_type_raw_text="ADULT, MALE, COMMUNITY",
                assessment_level=StateAssessmentLevel.LOW,
            ),
            StateAssessment.new_with_defaults(
                state_code=StateCode.US_ME.value,
                assessment_date=date(2010, 1, 1),
                assessment_type=StateAssessmentType.LSIR,
                assessment_type_raw_text="ADULT, MALE, COMMUNITY",
                assessment_level=StateAssessmentLevel.MAXIMUM,
            ),
            StateAssessment.new_with_defaults(
                state_code=StateCode.US_ME.value,
                assessment_date=date(2010, 1, 1),
                assessment_type=StateAssessmentType.STATIC_99,
                assessment_type_raw_text="STATIC 99 R",
                assessment_level=StateAssessmentLevel.MAXIMUM,
            ),
        ]
        self.assertEqual(
            StateSupervisionLevel.MEDIUM,
            self._build_delegate(assessments).supervision_level_override(
                supervision_period
            ),
        )

    def test_supervision_level_override_most_recent_lsir(self) -> None:
        """Assert that the most recent LSIR assessment level is used if no Static 99 is present."""
        supervision_period = StateSupervisionPeriod.new_with_defaults(
            state_code=StateCode.US_ME.value,
            start_date=date(2010, 1, 1),
            termination_date=date(2010, 3, 30),
        )
        assessments = [
            StateAssessment.new_with_defaults(
                state_code=StateCode.US_ME.value,
                assessment_date=date(2010, 3, 15),
                assessment_type=StateAssessmentType.LSIR,
                assessment_type_raw_text="ADULT, MALE, COMMUNITY",
                assessment_level=StateAssessmentLevel.LOW,
            ),
            StateAssessment.new_with_defaults(
                state_code=StateCode.US_ME.value,
                assessment_date=date(2010, 1, 1),
                assessment_type=StateAssessmentType.LSIR,
                assessment_type_raw_text="ADULT, MALE, COMMUNITY",
                assessment_level=StateAssessmentLevel.MAXIMUM,
            ),
        ]
        self.assertEqual(
            StateSupervisionLevel.MINIMUM,
            self._build_delegate(assessments).supervision_level_override(
                supervision_period
            ),
        )

    def test_supervision_termination_reason_override_revocation_sentence_future_date(
        self,
    ) -> None:
        supervision_period = StateSupervisionPeriod.new_with_defaults(
            state_code=StateCode.US_ME.value,
            start_date=date(2009, 1, 1),
            termination_date=date(2010, 1, 7),
            termination_reason=StateSupervisionPeriodTerminationReason.RETURN_TO_INCARCERATION,
        )
        supervision_sentences = [
            StateSupervisionSentence.new_with_defaults(
                state_code=StateCode.US_ME.value,
                completion_date=date(2010, 3, 1),
                status=StateSentenceStatus.COMPLETED,
            ),
            StateSupervisionSentence.new_with_defaults(
                state_code=StateCode.US_ME.value,
                completion_date=date(2010, 1, 2),
                status=StateSentenceStatus.REVOKED,
            ),
        ]
        self.assertEqual(
            StateSupervisionPeriodTerminationReason.REVOCATION,
            self._build_delegate(
                assessments=[]
            ).supervision_termination_reason_override(
                supervision_period, supervision_sentences
            ),
        )

    def test_supervision_termination_reason_override_revocation_sentence_past_date(
        self,
    ) -> None:
        supervision_period = StateSupervisionPeriod.new_with_defaults(
            state_code=StateCode.US_ME.value,
            start_date=date(2009, 1, 1),
            termination_date=date(2009, 12, 26),
            termination_reason=StateSupervisionPeriodTerminationReason.RETURN_TO_INCARCERATION,
        )
        supervision_sentences = [
            StateSupervisionSentence.new_with_defaults(
                state_code=StateCode.US_ME.value,
                completion_date=date(2010, 3, 1),
                status=StateSentenceStatus.COMPLETED,
            ),
            StateSupervisionSentence.new_with_defaults(
                state_code=StateCode.US_ME.value,
                completion_date=date(2010, 1, 2),
                status=StateSentenceStatus.REVOKED,
            ),
        ]
        self.assertEqual(
            StateSupervisionPeriodTerminationReason.REVOCATION,
            self._build_delegate(
                assessments=[]
            ).supervision_termination_reason_override(
                supervision_period, supervision_sentences
            ),
        )

    def test_supervision_termination_reason_override_no_revocation_sentence(
        self,
    ) -> None:
        supervision_period = StateSupervisionPeriod.new_with_defaults(
            state_code=StateCode.US_ME.value,
            start_date=date(2009, 1, 1),
            termination_date=date(2010, 1, 7),
            termination_reason=StateSupervisionPeriodTerminationReason.TRANSFER_WITHIN_STATE,
        )
        supervision_sentences = [
            StateSupervisionSentence.new_with_defaults(
                state_code=StateCode.US_ME.value,
                completion_date=date(2010, 3, 1),
                status=StateSentenceStatus.COMPLETED,
            ),
            StateSupervisionSentence.new_with_defaults(
                state_code=StateCode.US_ME.value,
                completion_date=date(2010, 1, 2),
                status=StateSentenceStatus.COMPLETED,
            ),
        ]
        self.assertEqual(
            StateSupervisionPeriodTerminationReason.TRANSFER_WITHIN_STATE,
            self._build_delegate(
                assessments=[]
            ).supervision_termination_reason_override(
                supervision_period, supervision_sentences
            ),
        )

    def test_supervision_termination_reason_override_no_sentences(
        self,
    ) -> None:
        supervision_period = StateSupervisionPeriod.new_with_defaults(
            state_code=StateCode.US_ME.value,
            start_date=date(2009, 1, 1),
            termination_date=date(2010, 1, 7),
            termination_reason=StateSupervisionPeriodTerminationReason.RETURN_TO_INCARCERATION,
        )
        supervision_sentences: List[StateSupervisionSentence] = []
        self.assertEqual(
            StateSupervisionPeriodTerminationReason.RETURN_TO_INCARCERATION,
            self._build_delegate(
                assessments=[]
            ).supervision_termination_reason_override(
                supervision_period, supervision_sentences
            ),
        )
