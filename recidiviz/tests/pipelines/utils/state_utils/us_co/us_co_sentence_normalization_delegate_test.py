#  Recidiviz - a data platform for criminal justice reform
#  Copyright (C) 2021 Recidiviz, Inc.
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
"""Tests us_co_sentence_normalization_delegate.py."""
import datetime
import unittest

from recidiviz.common.constants.state.state_charge import StateChargeStatus
from recidiviz.common.constants.state.state_sentence import StateSentenceStatus
from recidiviz.common.constants.states import StateCode
from recidiviz.persistence.entity.state.entities import (
    StateCharge,
    StateIncarcerationSentence,
)
from recidiviz.pipelines.utils.state_utils.us_co.us_co_sentence_normalization_delegate import (
    UsCoSentenceNormalizationDelegate,
)

_STATE_CODE = StateCode.US_CO.value


class TestUsCoSentenceNormalizationDelegate(unittest.TestCase):
    """Tests functions in UsCoSentenceNormalizationDelegate."""

    def setUp(self) -> None:
        self.delegate = UsCoSentenceNormalizationDelegate()

    def test_replaces_invalid_dates_incarceration_sentence(self) -> None:
        incarceration_sentence = StateIncarcerationSentence.new_with_defaults(
            state_code=_STATE_CODE,
            external_id="is1",
            date_imposed=datetime.date(1000, 1, 1),
            effective_date=datetime.date(934, 3, 3),
            completion_date=datetime.date(1000, 1, 1),
            status=StateSentenceStatus.PRESENT_WITHOUT_INFO,
        )
        result = self.delegate.update_incarceration_sentence(incarceration_sentence)
        self.assertIsNone(result.date_imposed)
        self.assertIsNone(result.effective_date)
        self.assertIsNone(result.completion_date)

    def test_replaces_invalid_dates_charges_preserving_relationship(self) -> None:
        charge = StateCharge.new_with_defaults(
            state_code=_STATE_CODE,
            external_id="c1",
            status=StateChargeStatus.PRESENT_WITHOUT_INFO,
            date_charged=datetime.date(1000, 1, 1),
        )
        incarceration_sentence = StateIncarcerationSentence.new_with_defaults(
            state_code=_STATE_CODE,
            external_id="is1",
            status=StateSentenceStatus.PRESENT_WITHOUT_INFO,
            charges=[charge],
        )
        charge.incarceration_sentences = [incarceration_sentence]

        result = self.delegate.update_incarceration_sentence(incarceration_sentence)
        self.assertIsNone(result.charges[0].date_charged)

    def test_does_not_replace_dates_if_valid_incarceration_sentence(self) -> None:
        incarceration_sentence = StateIncarcerationSentence.new_with_defaults(
            state_code=_STATE_CODE,
            external_id="is1",
            date_imposed=datetime.date(2000, 1, 1),
            status=StateSentenceStatus.PRESENT_WITHOUT_INFO,
        )
        result = self.delegate.update_incarceration_sentence(incarceration_sentence)
        self.assertEqual(result, incarceration_sentence)
