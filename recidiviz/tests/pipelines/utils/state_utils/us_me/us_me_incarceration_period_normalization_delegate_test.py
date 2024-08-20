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
"""Tests us_me_incarceration_period_normalization_delegate.py."""
import unittest
from datetime import date

from recidiviz.common.constants.state.state_incarceration_period import (
    StateIncarcerationPeriodAdmissionReason,
)
from recidiviz.common.constants.state.state_sentence import StateSentenceStatus
from recidiviz.common.constants.states import StateCode
from recidiviz.persistence.entity.state.entities import StateIncarcerationPeriod
from recidiviz.persistence.entity.state.normalized_entities import (
    NormalizedStateIncarcerationSentence,
)
from recidiviz.pipelines.utils.state_utils.us_me.us_me_incarceration_period_normalization_delegate import (
    UsMeIncarcerationNormalizationDelegate,
)

_STATE_CODE = StateCode.US_ME.value


class TestUsMeIncarcerationNormalizationDelegate(unittest.TestCase):
    """Tests functions in TestUsMeIncarcerationNormalizationDelegate."""

    def test_incarceration_admission_reason_override_revocation(
        self,
    ) -> None:
        incarceration_period = StateIncarcerationPeriod.new_with_defaults(
            state_code=StateCode.US_ME.value,
            external_id="ip1",
            admission_date=date(2009, 1, 10),
        )
        incarceration_sentences = [
            NormalizedStateIncarcerationSentence(
                incarceration_sentence_id=1,
                state_code=StateCode.US_ME.value,
                external_id="is1",
                status=StateSentenceStatus.COMPLETED,
                sentence_metadata='{"CONSECUTIVE_SENTENCE_ID": "", '
                '"TERM_COMMUNITY_RELEASE_DATE": "", '
                '"TERM_EARLY_CUSTODY_RELEASE_DATE": "2009-12-22 00:00:00", '
                '"TERM_INTAKE_DATE": "2009-01-03 10:32:00", '
                '"TERM_MAX_CUSTODY_RELEASE_DATE": "2010-04-16 00:00:00", '
                '"TERM_STATUS": "COMPLETE",'
                '"IS_REVOCATION_SENTENCE": "Y"}',
            ),
        ]
        delegate = UsMeIncarcerationNormalizationDelegate(incarceration_sentences)
        self.assertEqual(
            StateIncarcerationPeriodAdmissionReason.REVOCATION,
            delegate.incarceration_admission_reason_override(incarceration_period),
        )

    def test_incarceration_admission_reason_override_no_revocation_within_date_range(
        self,
    ) -> None:
        incarceration_period = StateIncarcerationPeriod.new_with_defaults(
            state_code=StateCode.US_ME.value,
            external_id="ip1",
            admission_date=date(2009, 1, 10),
            admission_reason=StateIncarcerationPeriodAdmissionReason.TRANSFER,
        )
        incarceration_sentences = [
            NormalizedStateIncarcerationSentence(
                incarceration_sentence_id=1,
                state_code=StateCode.US_ME.value,
                external_id="is1",
                status=StateSentenceStatus.COMPLETED,
                sentence_metadata='{"CONSECUTIVE_SENTENCE_ID": "", '
                '"TERM_COMMUNITY_RELEASE_DATE": "", '
                '"TERM_EARLY_CUSTODY_RELEASE_DATE": "2009-12-22 00:00:00", '
                '"TERM_INTAKE_DATE": "2015-01-03 10:32:00", '
                '"TERM_MAX_CUSTODY_RELEASE_DATE": "2010-04-16 00:00:00", '
                '"TERM_STATUS": "COMPLETE",'
                '"IS_REVOCATION_SENTENCE": "Y"}',
            ),
        ]
        delegate = UsMeIncarcerationNormalizationDelegate(incarceration_sentences)
        self.assertEqual(
            StateIncarcerationPeriodAdmissionReason.TRANSFER,
            delegate.incarceration_admission_reason_override(incarceration_period),
        )

    def test_incarceration_admission_reason_override_not_revocation(
        self,
    ) -> None:
        incarceration_period = StateIncarcerationPeriod.new_with_defaults(
            state_code=StateCode.US_ME.value,
            external_id="ip1",
            admission_date=date(2009, 1, 10),
            admission_reason=StateIncarcerationPeriodAdmissionReason.NEW_ADMISSION,
        )
        incarceration_sentences = [
            NormalizedStateIncarcerationSentence(
                incarceration_sentence_id=1,
                state_code=StateCode.US_ME.value,
                external_id="is1",
                status=StateSentenceStatus.COMPLETED,
                sentence_metadata='{"CONSECUTIVE_SENTENCE_ID": "", '
                '"TERM_COMMUNITY_RELEASE_DATE": "", '
                '"TERM_EARLY_CUSTODY_RELEASE_DATE": "2009-12-22 00:00:00", '
                '"TERM_INTAKE_DATE": "2009-01-10 10:32:00", '
                '"TERM_MAX_CUSTODY_RELEASE_DATE": "2010-04-16 00:00:00", '
                '"TERM_STATUS": "COMPLETE",'
                '"IS_REVOCATION_SENTENCE": "N"}',
            ),
        ]
        delegate = UsMeIncarcerationNormalizationDelegate(incarceration_sentences)
        self.assertEqual(
            StateIncarcerationPeriodAdmissionReason.NEW_ADMISSION,
            delegate.incarceration_admission_reason_override(incarceration_period),
        )
