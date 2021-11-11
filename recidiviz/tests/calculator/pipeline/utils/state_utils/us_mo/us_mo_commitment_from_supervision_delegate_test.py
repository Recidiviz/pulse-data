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
"""Tests for the us_mo_commitment_from_supervision_delegate.py file"""
import datetime
import unittest

from recidiviz.calculator.pipeline.utils.state_utils.us_mo.us_mo_commitment_from_supervision_delegate import (
    UsMoCommitmentFromSupervisionDelegate,
)
from recidiviz.calculator.pipeline.utils.state_utils.us_mo.us_mo_sentence_classification import (
    SupervisionTypeSpan,
)
from recidiviz.common.constants.state.state_incarceration_period import (
    StateIncarcerationPeriodAdmissionReason,
    StateIncarcerationPeriodStatus,
)
from recidiviz.common.constants.state.state_sentence import StateSentenceStatus
from recidiviz.common.constants.state.state_supervision_period import (
    StateSupervisionPeriodSupervisionType,
)
from recidiviz.common.constants.state.state_supervision_sentence import (
    StateSupervisionSentenceSupervisionType,
)
from recidiviz.persistence.entity.state.entities import (
    StateIncarcerationPeriod,
    StateIncarcerationSentence,
    StateSupervisionSentence,
)
from recidiviz.tests.calculator.pipeline.utils.us_mo_fakes import (
    FakeUsMoIncarcerationSentence,
    FakeUsMoSupervisionSentence,
)


class TestUsMoCommitmentFromSupervisionDelegate(unittest.TestCase):
    """Unit tests for UsMoCommitmentFromSupervisionDelegate"""

    def setUp(self) -> None:
        self.delegate = UsMoCommitmentFromSupervisionDelegate()

    def test_get_commitment_from_supervision_supervision_type(self) -> None:
        incarceration_period = StateIncarcerationPeriod.new_with_defaults(
            incarceration_period_id=1,
            admission_reason=StateIncarcerationPeriodAdmissionReason.PAROLE_REVOCATION,
            external_id="ip1",
            state_code="US_MO",
            admission_date=datetime.date(year=2019, month=9, day=13),
            status=StateIncarcerationPeriodStatus.PRESENT_WITHOUT_INFO,
        )

        supervision_sentence_parole = (
            FakeUsMoSupervisionSentence.fake_sentence_from_sentence(
                StateSupervisionSentence.new_with_defaults(
                    state_code="US_MO",
                    supervision_sentence_id=1,
                    external_id="1167633-20171012-2",
                    start_date=datetime.date(2017, 2, 1),
                    supervision_type=StateSupervisionSentenceSupervisionType.PROBATION,
                    status=StateSentenceStatus.PRESENT_WITHOUT_INFO,
                ),
                supervision_type_spans=[
                    SupervisionTypeSpan(
                        start_date=datetime.date(2017, 2, 1),
                        end_date=None,
                        supervision_type=StateSupervisionSentenceSupervisionType.PAROLE,
                        start_critical_statuses=[],
                        end_critical_statuses=[],
                    )
                ],
            )
        )

        # Even though the supervision type of the sentence is PROBATION, we find that it's actually a PAROLE
        # sentence from the statuses.
        self.assertEqual(
            StateSupervisionPeriodSupervisionType.PAROLE,
            self.delegate.get_commitment_from_supervision_supervision_type(
                incarceration_sentences=[],
                supervision_sentences=[supervision_sentence_parole],
                incarceration_period=incarceration_period,
                previous_supervision_period=None,
            ),
        )

    def test_get_commitment_from_supervision_supervision_type_ignore_out_of_date_sentences(
        self,
    ) -> None:
        incarceration_period = StateIncarcerationPeriod.new_with_defaults(
            incarceration_period_id=1,
            admission_reason=StateIncarcerationPeriodAdmissionReason.PAROLE_REVOCATION,
            external_id="ip1",
            state_code="US_MO",
            admission_date=datetime.date(year=2019, month=9, day=13),
            status=StateIncarcerationPeriodStatus.PRESENT_WITHOUT_INFO,
        )

        supervision_sentence_parole = (
            FakeUsMoSupervisionSentence.fake_sentence_from_sentence(
                StateSupervisionSentence.new_with_defaults(
                    state_code="US_MO",
                    supervision_sentence_id=1,
                    external_id="1167633-20171012-2",
                    start_date=datetime.date(2017, 2, 1),
                    supervision_type=StateSupervisionSentenceSupervisionType.PROBATION,
                    status=StateSentenceStatus.PRESENT_WITHOUT_INFO,
                ),
                supervision_type_spans=[
                    SupervisionTypeSpan(
                        start_date=datetime.date(2017, 2, 1),
                        end_date=None,
                        supervision_type=StateSupervisionSentenceSupervisionType.PAROLE,
                        start_critical_statuses=[],
                        end_critical_statuses=[],
                    )
                ],
            )
        )

        old_incarceration_sentence = (
            FakeUsMoIncarcerationSentence.fake_sentence_from_sentence(
                StateIncarcerationSentence.new_with_defaults(
                    state_code="US_MO",
                    incarceration_sentence_id=1,
                    external_id="is1",
                    start_date=datetime.date(2017, 2, 1),
                    completion_date=datetime.date(2017, 3, 4),
                    status=StateSentenceStatus.COMPLETED,
                ),
                supervision_type_spans=[
                    SupervisionTypeSpan(
                        start_date=datetime.date(2017, 2, 1),
                        end_date=datetime.date(2017, 3, 4),
                        supervision_type=StateSupervisionSentenceSupervisionType.PAROLE,
                        start_critical_statuses=[],
                        end_critical_statuses=[],
                    ),
                    SupervisionTypeSpan(
                        start_date=datetime.date(2017, 3, 4),
                        end_date=None,
                        supervision_type=None,
                        start_critical_statuses=[],
                        end_critical_statuses=None,
                    ),
                ],  # Terminated by revocation date
            )
        )

        self.assertEqual(
            StateSupervisionPeriodSupervisionType.PAROLE,
            self.delegate.get_commitment_from_supervision_supervision_type(
                [old_incarceration_sentence],
                [supervision_sentence_parole],
                incarceration_period,
                previous_supervision_period=None,
            ),
        )
