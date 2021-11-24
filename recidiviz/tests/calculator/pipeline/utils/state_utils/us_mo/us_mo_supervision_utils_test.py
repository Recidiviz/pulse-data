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
"""Tests for us_mo_supervision_utils.py"""
import datetime
import unittest

from recidiviz.calculator.pipeline.utils.state_utils.us_mo.us_mo_sentence_classification import (
    SupervisionTypeSpan,
)
from recidiviz.calculator.pipeline.utils.state_utils.us_mo.us_mo_supervision_utils import (
    us_mo_get_most_recent_supervision_type_before_upper_bound_day,
    us_mo_get_post_incarceration_supervision_type,
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


class UsMoGetPostIncarcerationSupervisionTypeTest(unittest.TestCase):
    """Tests for us_mo_get_post_incarceration_supervision_type"""

    def test_usMo_getPostIncarcerationSupervisionType(self) -> None:
        incarceration_period = StateIncarcerationPeriod.new_with_defaults(
            incarceration_period_id=1,
            admission_reason=StateIncarcerationPeriodAdmissionReason.PAROLE_REVOCATION,
            external_id="ip1",
            state_code="US_MO",
            admission_date=datetime.date(year=2019, month=9, day=13),
            release_date=datetime.date(year=2020, month=1, day=11),
            status=StateIncarcerationPeriodStatus.PRESENT_WITHOUT_INFO,
        )

        supervision_sentence_parole = (
            FakeUsMoSupervisionSentence.fake_sentence_from_sentence(
                StateSupervisionSentence.new_with_defaults(
                    state_code="US_MO",
                    supervision_sentence_id=1,
                    external_id="1167633-20171012-2",
                    start_date=datetime.date(year=2020, month=1, day=11),
                    supervision_type=StateSupervisionSentenceSupervisionType.PROBATION,
                    status=StateSentenceStatus.PRESENT_WITHOUT_INFO,
                ),
                supervision_type_spans=[
                    SupervisionTypeSpan(
                        start_date=datetime.date(year=2020, month=1, day=11),
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
            us_mo_get_post_incarceration_supervision_type(
                incarceration_sentences=[],
                supervision_sentences=[supervision_sentence_parole],
                incarceration_period=incarceration_period,
            ),
        )

    def test_usMo_getPostIncarcerationSupervisionType_ignoreOutOfDateSentences(
        self,
    ) -> None:
        incarceration_period = StateIncarcerationPeriod.new_with_defaults(
            incarceration_period_id=1,
            admission_reason=StateIncarcerationPeriodAdmissionReason.PAROLE_REVOCATION,
            external_id="ip1",
            state_code="US_MO",
            admission_date=datetime.date(year=2019, month=9, day=13),
            release_date=datetime.date(year=2020, month=1, day=11),
            status=StateIncarcerationPeriodStatus.PRESENT_WITHOUT_INFO,
        )

        supervision_sentence_parole = (
            FakeUsMoSupervisionSentence.fake_sentence_from_sentence(
                StateSupervisionSentence.new_with_defaults(
                    state_code="US_MO",
                    supervision_sentence_id=1,
                    external_id="1167633-20171012-2",
                    start_date=datetime.date(2020, 1, 13),
                    supervision_type=StateSupervisionSentenceSupervisionType.PROBATION,
                    status=StateSentenceStatus.PRESENT_WITHOUT_INFO,
                ),
                supervision_type_spans=[
                    SupervisionTypeSpan(
                        start_date=datetime.date(2020, 1, 13),
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
                ],
            )
        )

        self.assertEqual(
            StateSupervisionPeriodSupervisionType.PAROLE,
            us_mo_get_post_incarceration_supervision_type(
                [old_incarceration_sentence],
                [supervision_sentence_parole],
                incarceration_period,
            ),
        )


class UsMoGetMostRecentSupervisionPeriodSupervisionTypeBeforeUpperBoundDayTest(
    unittest.TestCase
):
    """Unittests for the us_mo_get_most_recent_supervision_type_before_upper_bound_day helper."""

    def setUp(self) -> None:
        self.upper_bound_date = datetime.date(2018, 10, 10)

    def test_most_recent_supervision_type_no_sentences_no_bound(self) -> None:
        # Act
        supervision_type = (
            us_mo_get_most_recent_supervision_type_before_upper_bound_day(
                upper_bound_exclusive_date=self.upper_bound_date,
                lower_bound_inclusive_date=None,
                supervision_sentences=[],
                incarceration_sentences=[],
            )
        )

        # Assert
        self.assertEqual(supervision_type, None)

    def test_most_recent_supervision_type_no_sentences_same_day_bound(self) -> None:
        # Act
        supervision_type = (
            us_mo_get_most_recent_supervision_type_before_upper_bound_day(
                upper_bound_exclusive_date=self.upper_bound_date,
                lower_bound_inclusive_date=self.upper_bound_date,
                supervision_sentences=[],
                incarceration_sentences=[],
            )
        )

        # Assert
        self.assertEqual(supervision_type, None)

    def test_most_recent_supervision_type_two_sentences_same_transition_day_one_different(
        self,
    ) -> None:
        # Arrange

        start_date = self.upper_bound_date - datetime.timedelta(days=5)
        transition_date_1 = self.upper_bound_date - datetime.timedelta(days=3)
        transition_date_2 = self.upper_bound_date - datetime.timedelta(days=1)

        incarceration_sentence = (
            FakeUsMoIncarcerationSentence.fake_sentence_from_sentence(
                StateIncarcerationSentence.new_with_defaults(
                    external_id="ss1",
                    state_code="US_MO",
                    start_date=start_date,
                    status=StateSentenceStatus.PRESENT_WITHOUT_INFO,
                ),
                supervision_type_spans=[
                    SupervisionTypeSpan(
                        start_date=start_date,
                        end_date=transition_date_2,
                        supervision_type=StateSupervisionSentenceSupervisionType.PAROLE,
                        start_critical_statuses=[],
                        end_critical_statuses=[],
                    ),
                    SupervisionTypeSpan(
                        start_date=transition_date_2,
                        end_date=None,
                        supervision_type=None,
                        start_critical_statuses=[],
                        end_critical_statuses=None,
                    ),
                ],
            )
        )

        supervision_sentence_1 = FakeUsMoSupervisionSentence.fake_sentence_from_sentence(
            StateSupervisionSentence.new_with_defaults(
                external_id="ss1",
                state_code="US_MO",
                start_date=start_date,
                status=StateSentenceStatus.PRESENT_WITHOUT_INFO,
            ),
            supervision_type_spans=[
                SupervisionTypeSpan(
                    start_date=start_date,
                    end_date=transition_date_1,
                    supervision_type=StateSupervisionSentenceSupervisionType.PROBATION,
                    start_critical_statuses=[],
                    end_critical_statuses=[],
                ),
                SupervisionTypeSpan(
                    start_date=transition_date_1,
                    end_date=None,
                    supervision_type=None,
                    start_critical_statuses=[],
                    end_critical_statuses=None,
                ),
            ],
        )

        supervision_sentence_2 = FakeUsMoSupervisionSentence.fake_sentence_from_sentence(
            StateSupervisionSentence.new_with_defaults(
                external_id="ss1",
                state_code="US_MO",
                start_date=start_date,
                status=StateSentenceStatus.PRESENT_WITHOUT_INFO,
            ),
            supervision_type_spans=[
                SupervisionTypeSpan(
                    start_date=start_date,
                    end_date=transition_date_2,
                    supervision_type=StateSupervisionSentenceSupervisionType.PROBATION,
                    start_critical_statuses=[],
                    end_critical_statuses=[],
                ),
                SupervisionTypeSpan(
                    start_date=transition_date_2,
                    end_date=None,
                    supervision_type=StateSupervisionSentenceSupervisionType.PAROLE,
                    start_critical_statuses=[],
                    end_critical_statuses=[],
                ),
            ],
        )

        # Act

        supervision_type = (
            us_mo_get_most_recent_supervision_type_before_upper_bound_day(
                upper_bound_exclusive_date=self.upper_bound_date,
                lower_bound_inclusive_date=None,
                supervision_sentences=[supervision_sentence_1, supervision_sentence_2],
                incarceration_sentences=[incarceration_sentence],
            )
        )

        # Assert

        # Since the probation sentence ends before the parole sentence, the last valid supervision type is PAROLE
        self.assertEqual(
            supervision_type,
            StateSupervisionPeriodSupervisionType.PAROLE,
        )
