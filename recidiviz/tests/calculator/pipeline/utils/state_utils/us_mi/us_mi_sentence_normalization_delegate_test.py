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
""""Tests the us_mi_sentence_normalization_delegate.py file."""
import unittest
from datetime import date

from recidiviz.calculator.pipeline.utils.state_utils.us_mi.us_mi_sentence_normalization_delegate import (
    UsMiSentenceNormalizationDelegate,
)
from recidiviz.common.constants.state.state_sentence import StateSentenceStatus
from recidiviz.common.constants.states import StateCode
from recidiviz.persistence.entity.state.entities import StateIncarcerationSentence

_STATE_CODE = StateCode.US_MI.value


class TestUsMiSentenceNormalizationDelegate(unittest.TestCase):
    """Tests the us_mi_sentence_normalization_delegate."""

    def setUp(self) -> None:
        self.delegate = UsMiSentenceNormalizationDelegate()

    def test_projected_max_completion_date_imputation(
        self,
    ) -> None:
        incarceration_sentence = StateIncarcerationSentence.new_with_defaults(
            state_code=_STATE_CODE,
            projected_max_release_date=None,
            effective_date=date(2023, 1, 1),
            max_length_days=365,
            status=StateSentenceStatus.PRESENT_WITHOUT_INFO,
        )

        result = self.delegate.update_incarceration_sentence(incarceration_sentence)
        self.assertEqual(result.projected_max_release_date, date(2024, 1, 1))

    def test_projected_max_completion_date_imputation_null_effective(
        self,
    ) -> None:
        incarceration_sentence = StateIncarcerationSentence.new_with_defaults(
            state_code=_STATE_CODE,
            projected_max_release_date=None,
            effective_date=None,
            max_length_days=365,
            status=StateSentenceStatus.PRESENT_WITHOUT_INFO,
        )

        result = self.delegate.update_incarceration_sentence(incarceration_sentence)
        self.assertIsNone(result.projected_max_release_date)

    def test_projected_max_completion_date_imputation_null_max_length(
        self,
    ) -> None:
        incarceration_sentence = StateIncarcerationSentence.new_with_defaults(
            state_code=_STATE_CODE,
            projected_max_release_date=None,
            effective_date=date(2023, 1, 1),
            max_length_days=None,
            status=StateSentenceStatus.PRESENT_WITHOUT_INFO,
        )

        result = self.delegate.update_incarceration_sentence(incarceration_sentence)
        self.assertIsNone(result.projected_max_release_date)

    def test_projected_max_completion_date_no_imputation(
        self,
    ) -> None:
        incarceration_sentence = StateIncarcerationSentence.new_with_defaults(
            state_code=_STATE_CODE,
            projected_max_release_date=date(2023, 7, 1),
            effective_date=date(2023, 1, 1),
            max_length_days=365,
            status=StateSentenceStatus.PRESENT_WITHOUT_INFO,
        )

        result = self.delegate.update_incarceration_sentence(incarceration_sentence)
        self.assertEqual(result.projected_max_release_date, date(2023, 7, 1))
