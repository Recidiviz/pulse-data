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
"""Tests for us_mo_supervision_type_identification.py"""
import datetime
import unittest
from typing import Optional

from recidiviz.calculator.pipeline.utils.state_utils.us_mo.us_mo_sentence_classification import \
    UsMoIncarcerationSentence, UsMoSupervisionSentence, SupervisionTypeSpan
from recidiviz.calculator.pipeline.utils.state_utils.us_mo.us_mo_supervision_type_identification import \
    us_mo_get_supervision_period_supervision_type_on_date, us_mo_get_pre_incarceration_supervision_type, \
    us_mo_get_month_supervision_type, \
    us_mo_get_most_recent_supervision_period_supervision_type_before_upper_bound_day, \
    us_mo_get_post_incarceration_supervision_type
from recidiviz.common.constants.state.state_incarceration_period import StateIncarcerationPeriodAdmissionReason, \
    StateIncarcerationPeriodStatus
from recidiviz.common.constants.state.state_sentence import StateSentenceStatus
from recidiviz.common.constants.state.state_supervision import StateSupervisionType
from recidiviz.common.constants.state.state_supervision_period import StateSupervisionPeriodSupervisionType, \
    StateSupervisionPeriodStatus
from recidiviz.persistence.entity.state.entities import StateIncarcerationSentence, StateSupervisionSentence, \
    StateIncarcerationPeriod, StateSupervisionPeriod
from recidiviz.tests.calculator.pipeline.utils.us_mo_fakes import FakeUsMoSupervisionSentence, \
    FakeUsMoIncarcerationSentence


class UsMoGetPreIncarcerationSupervisionTypeTest(unittest.TestCase):
    """Tests for us_mo_get_pre_incarceration_supervision_type"""

    def test_usMo_getPreIncarcerationSupervisionType(self):
        incarceration_period = StateIncarcerationPeriod.new_with_defaults(
            incarceration_period_id=1,
            admission_reason=StateIncarcerationPeriodAdmissionReason.PAROLE_REVOCATION,
            external_id='ip1',
            state_code='US_MO',
            admission_date=datetime.date(year=2019, month=9, day=13),
            status=StateIncarcerationPeriodStatus.PRESENT_WITHOUT_INFO)

        supervision_sentence_parole = FakeUsMoSupervisionSentence.fake_sentence_from_sentence(
            StateSupervisionSentence.new_with_defaults(
                state_code='US_MO',
                supervision_sentence_id=1,
                external_id='1167633-20171012-2',
                start_date=datetime.date(2017, 2, 1),
                supervision_type=StateSupervisionType.PROBATION),
            supervision_type_spans=[SupervisionTypeSpan(
                start_date=datetime.date(2017, 2, 1),
                end_date=None,
                supervision_type=StateSupervisionType.PAROLE
            )]
        )

        # Even though the supervision type of the sentence is PROBATION, we find that it's actually a PAROLE
        # sentence from the statuses.
        self.assertEqual(StateSupervisionPeriodSupervisionType.PAROLE,
                         us_mo_get_pre_incarceration_supervision_type(
                             incarceration_sentences=[],
                             supervision_sentences=[supervision_sentence_parole],
                             incarceration_period=incarceration_period))

    def test_usMo_getPreIncarcerationSupervisionType_ignoreOutOfDateSentences(self):
        incarceration_period = StateIncarcerationPeriod.new_with_defaults(
            incarceration_period_id=1,
            admission_reason=StateIncarcerationPeriodAdmissionReason.PAROLE_REVOCATION,
            external_id='ip1',
            state_code='US_MO',
            admission_date=datetime.date(year=2019, month=9, day=13),
            status=StateIncarcerationPeriodStatus.PRESENT_WITHOUT_INFO)

        supervision_sentence_parole = FakeUsMoSupervisionSentence.fake_sentence_from_sentence(
            StateSupervisionSentence.new_with_defaults(
                state_code='US_MO',
                supervision_sentence_id=1,
                external_id='1167633-20171012-2',
                start_date=datetime.date(2017, 2, 1),
                supervision_type=StateSupervisionType.PROBATION),
            supervision_type_spans=[SupervisionTypeSpan(
                start_date=datetime.date(2017, 2, 1),
                end_date=None,
                supervision_type=StateSupervisionType.PAROLE
            )]
        )

        old_incarceration_sentence = FakeUsMoIncarcerationSentence.fake_sentence_from_sentence(
            StateIncarcerationSentence.new_with_defaults(
                state_code='US_MO',
                incarceration_sentence_id=1,
                external_id='is1',
                start_date=datetime.date(2017, 2, 1),
                completion_date=datetime.date(2017, 3, 4),
                status=StateSentenceStatus.COMPLETED),
            supervision_type_spans=[
                SupervisionTypeSpan(
                    start_date=datetime.date(2017, 2, 1),
                    end_date=datetime.date(2017, 3, 4),
                    supervision_type=StateSupervisionType.PAROLE
                ),
                SupervisionTypeSpan(
                    start_date=datetime.date(2017, 3, 4),
                    end_date=None,
                    supervision_type=None
                )
            ]  # Terminated by revocation date
        )

        self.assertEqual(StateSupervisionPeriodSupervisionType.PAROLE,
                         us_mo_get_pre_incarceration_supervision_type(
                             [old_incarceration_sentence],
                             [supervision_sentence_parole],
                             incarceration_period))


class UsMoGetPostIncarcerationSupervisionTypeTest(unittest.TestCase):
    """Tests for us_mo_get_post_incarceration_supervision_type"""

    def test_usMo_getPostIncarcerationSupervisionType(self):
        incarceration_period = StateIncarcerationPeriod.new_with_defaults(
            incarceration_period_id=1,
            admission_reason=StateIncarcerationPeriodAdmissionReason.PAROLE_REVOCATION,
            external_id='ip1',
            state_code='US_MO',
            admission_date=datetime.date(year=2019, month=9, day=13),
            release_date=datetime.date(year=2020, month=1, day=11),
            status=StateIncarcerationPeriodStatus.PRESENT_WITHOUT_INFO
        )

        supervision_sentence_parole = FakeUsMoSupervisionSentence.fake_sentence_from_sentence(
            StateSupervisionSentence.new_with_defaults(
                state_code='US_MO',
                supervision_sentence_id=1,
                external_id='1167633-20171012-2',
                start_date=datetime.date(year=2020, month=1, day=11),
                supervision_type=StateSupervisionType.PROBATION),
            supervision_type_spans=[SupervisionTypeSpan(
                start_date=datetime.date(year=2020, month=1, day=11),
                end_date=None,
                supervision_type=StateSupervisionType.PAROLE
            )]
        )

        # Even though the supervision type of the sentence is PROBATION, we find that it's actually a PAROLE
        # sentence from the statuses.
        self.assertEqual(StateSupervisionPeriodSupervisionType.PAROLE,
                         us_mo_get_post_incarceration_supervision_type(
                             incarceration_sentences=[],
                             supervision_sentences=[supervision_sentence_parole],
                             incarceration_period=incarceration_period))

    def test_usMo_getPostIncarcerationSupervisionType_ignoreOutOfDateSentences(self):
        incarceration_period = StateIncarcerationPeriod.new_with_defaults(
            incarceration_period_id=1,
            admission_reason=StateIncarcerationPeriodAdmissionReason.PAROLE_REVOCATION,
            external_id='ip1',
            state_code='US_MO',
            admission_date=datetime.date(year=2019, month=9, day=13),
            release_date=datetime.date(year=2020, month=1, day=11),
            status=StateIncarcerationPeriodStatus.PRESENT_WITHOUT_INFO
        )

        supervision_sentence_parole = FakeUsMoSupervisionSentence.fake_sentence_from_sentence(
            StateSupervisionSentence.new_with_defaults(
                state_code='US_MO',
                supervision_sentence_id=1,
                external_id='1167633-20171012-2',
                start_date=datetime.date(2020, 1, 13),
                supervision_type=StateSupervisionType.PROBATION),
            supervision_type_spans=[SupervisionTypeSpan(
                start_date=datetime.date(2020, 1, 13),
                end_date=None,
                supervision_type=StateSupervisionType.PAROLE
            )]
        )

        old_incarceration_sentence = FakeUsMoIncarcerationSentence.fake_sentence_from_sentence(
            StateIncarcerationSentence.new_with_defaults(
                state_code='US_MO',
                incarceration_sentence_id=1,
                external_id='is1',
                start_date=datetime.date(2017, 2, 1),
                completion_date=datetime.date(2017, 3, 4),
                status=StateSentenceStatus.COMPLETED),
            supervision_type_spans=[
                SupervisionTypeSpan(
                    start_date=datetime.date(2017, 2, 1),
                    end_date=datetime.date(2017, 3, 4),
                    supervision_type=StateSupervisionType.PAROLE
                ),
                SupervisionTypeSpan(
                    start_date=datetime.date(2017, 3, 4),
                    end_date=None,
                    supervision_type=None
                )
            ]
        )

        self.assertEqual(StateSupervisionPeriodSupervisionType.PAROLE,
                         us_mo_get_post_incarceration_supervision_type(
                             [old_incarceration_sentence],
                             [supervision_sentence_parole],
                             incarceration_period))


class UsMoGetSupervisionPeriodSupervisionTypeOnDateTest(unittest.TestCase):
    """Unittests for the us_mo_get_supervision_period_supervision_type_on_date helper."""
    def setUp(self) -> None:
        self.validation_date = datetime.date(year=2019, month=10, day=31)
        self.sentence_id_counter = 0

    def _inc_sentence_with_type(self, supervision_type: Optional[StateSupervisionType]) -> UsMoIncarcerationSentence:
        start_date = datetime.date(year=2012, month=6, day=27)
        base_sentence = StateIncarcerationSentence.new_with_defaults(
            external_id=f'164735-20120627-{self.sentence_id_counter}',
            state_code='US_MO',
            start_date=start_date,
            status=StateSentenceStatus.PRESENT_WITHOUT_INFO
        )
        mo_sentence = FakeUsMoIncarcerationSentence.fake_sentence_from_sentence(
            base_sentence,
            supervision_type_spans=[
                SupervisionTypeSpan(start_date=start_date,
                                    end_date=None,
                                    supervision_type=supervision_type)
            ])

        self.sentence_id_counter += 1

        return mo_sentence

    def _sup_sentence_with_type(self, supervision_type: Optional[StateSupervisionType]) -> UsMoSupervisionSentence:
        start_date = datetime.date(year=2012, month=6, day=27)
        base_sentence = StateSupervisionSentence.new_with_defaults(
            external_id=f'164735-20120627-{self.sentence_id_counter}',
            state_code='US_MO',
            start_date=datetime.date(year=2012, month=6, day=27)
        )
        mo_sentence = FakeUsMoSupervisionSentence.fake_sentence_from_sentence(
            base_sentence,
            supervision_type_spans=[
                SupervisionTypeSpan(start_date=start_date,
                                    end_date=None,
                                    supervision_type=supervision_type)
            ])

        self.sentence_id_counter += 1

        return mo_sentence

    def test_get_supervision_type_no_sentences(self):
        # Act
        supervision_period_supervision_type = \
            us_mo_get_supervision_period_supervision_type_on_date(self.validation_date,
                                                                  supervision_sentences=[],
                                                                  incarceration_sentences=[])

        # Assert
        self.assertEqual(supervision_period_supervision_type, None)

    def test_get_supervision_type_all_sentences_expired(self):
        # Arrange
        mo_incarceration_sentence = self._inc_sentence_with_type(None)
        mo_supervision_sentence = self._sup_sentence_with_type(None)

        # Act
        supervision_period_supervision_type = \
            us_mo_get_supervision_period_supervision_type_on_date(self.validation_date,
                                                                  supervision_sentences=[mo_supervision_sentence],
                                                                  incarceration_sentences=[mo_incarceration_sentence])

        # Assert
        self.assertEqual(supervision_period_supervision_type, None)

    def test_get_supervision_type_dual_with_incarceration_and_supervision_sentences(self):
        # Arrange
        mo_incarceration_sentence = self._inc_sentence_with_type(StateSupervisionType.PAROLE)
        mo_supervision_sentence = self._sup_sentence_with_type(StateSupervisionType.PROBATION)

        # Act
        supervision_period_supervision_type = \
            us_mo_get_supervision_period_supervision_type_on_date(self.validation_date,
                                                                  supervision_sentences=[mo_supervision_sentence],
                                                                  incarceration_sentences=[mo_incarceration_sentence])

        # Assert
        self.assertEqual(supervision_period_supervision_type, StateSupervisionPeriodSupervisionType.DUAL)

    def test_get_supervision_type_dual_with_just_supervision_sentences(self):
        # Arrange
        mo_supervision_sentence_1 = self._sup_sentence_with_type(StateSupervisionType.PAROLE)
        mo_supervision_sentence_2 = self._sup_sentence_with_type(StateSupervisionType.PROBATION)

        # Act
        supervision_period_supervision_type = \
            us_mo_get_supervision_period_supervision_type_on_date(self.validation_date,
                                                                  supervision_sentences=[mo_supervision_sentence_1,
                                                                                         mo_supervision_sentence_2],
                                                                  incarceration_sentences=[])

        # Assert
        self.assertEqual(supervision_period_supervision_type, StateSupervisionPeriodSupervisionType.DUAL)

    def test_get_supervision_type_parole_with_sentence_expiration(self):
        # Arrange
        mo_incarceration_sentence_1 = self._inc_sentence_with_type(None)
        mo_incarceration_sentence_2 = self._inc_sentence_with_type(StateSupervisionType.PAROLE)

        # Act
        supervision_period_supervision_type = \
            us_mo_get_supervision_period_supervision_type_on_date(self.validation_date,
                                                                  supervision_sentences=[],
                                                                  incarceration_sentences=[mo_incarceration_sentence_1,
                                                                                           mo_incarceration_sentence_2])

        # Assert
        self.assertEqual(supervision_period_supervision_type, StateSupervisionPeriodSupervisionType.PAROLE)

    def test_get_supervision_type_probation_with_sentence_expiration(self):
        # Arrange
        mo_incarceration_sentence = self._inc_sentence_with_type(None)
        mo_supervision_sentence = self._sup_sentence_with_type(StateSupervisionType.PROBATION)

        # Act
        supervision_period_supervision_type = \
            us_mo_get_supervision_period_supervision_type_on_date(self.validation_date,
                                                                  supervision_sentences=[mo_supervision_sentence],
                                                                  incarceration_sentences=[mo_incarceration_sentence])

        # Assert
        self.assertEqual(supervision_period_supervision_type, StateSupervisionPeriodSupervisionType.PROBATION)

    def test_get_supervision_type_dual_with_sentence_expiration(self):
        # Arrange
        mo_incarceration_sentence_1 = self._inc_sentence_with_type(None)
        mo_incarceration_sentence_2 = self._inc_sentence_with_type(StateSupervisionType.PAROLE)
        mo_supervision_sentence = self._sup_sentence_with_type(StateSupervisionType.PROBATION)

        # Act
        supervision_period_supervision_type = \
            us_mo_get_supervision_period_supervision_type_on_date(self.validation_date,
                                                                  supervision_sentences=[mo_supervision_sentence],
                                                                  incarceration_sentences=[mo_incarceration_sentence_1,
                                                                                           mo_incarceration_sentence_2])

        # Assert
        self.assertEqual(supervision_period_supervision_type, StateSupervisionPeriodSupervisionType.DUAL)

    def test_get_supervision_type_dual_with_internal_unknown(self):
        # Arrange
        mo_incarceration_sentence_1 = self._inc_sentence_with_type(StateSupervisionType.INTERNAL_UNKNOWN)
        mo_incarceration_sentence_2 = self._inc_sentence_with_type(StateSupervisionType.PAROLE)
        mo_supervision_sentence = self._sup_sentence_with_type(StateSupervisionType.PROBATION)

        # Act
        supervision_period_supervision_type = \
            us_mo_get_supervision_period_supervision_type_on_date(self.validation_date,
                                                                  supervision_sentences=[mo_supervision_sentence],
                                                                  incarceration_sentences=[mo_incarceration_sentence_1,
                                                                                           mo_incarceration_sentence_2])

        # Assert
        self.assertEqual(supervision_period_supervision_type, StateSupervisionPeriodSupervisionType.DUAL)

    def test_get_supervision_type_two_parole_sentences(self):
        # Arrange
        mo_incarceration_sentence_1 = self._inc_sentence_with_type(StateSupervisionType.PAROLE)
        mo_incarceration_sentence_2 = self._inc_sentence_with_type(StateSupervisionType.PAROLE)

        # Act
        supervision_period_supervision_type = \
            us_mo_get_supervision_period_supervision_type_on_date(self.validation_date,
                                                                  supervision_sentences=[],
                                                                  incarceration_sentences=[mo_incarceration_sentence_1,
                                                                                           mo_incarceration_sentence_2])

        # Assert
        self.assertEqual(supervision_period_supervision_type, StateSupervisionPeriodSupervisionType.PAROLE)

    def test_get_supervision_type_parole_with_internal_unknown(self):
        # Arrange
        mo_incarceration_sentence_1 = self._inc_sentence_with_type(StateSupervisionType.INTERNAL_UNKNOWN)
        mo_incarceration_sentence_2 = self._inc_sentence_with_type(StateSupervisionType.PAROLE)

        # Act
        supervision_period_supervision_type = \
            us_mo_get_supervision_period_supervision_type_on_date(self.validation_date,
                                                                  supervision_sentences=[],
                                                                  incarceration_sentences=[mo_incarceration_sentence_1,
                                                                                           mo_incarceration_sentence_2])

        # Assert
        self.assertEqual(supervision_period_supervision_type, StateSupervisionPeriodSupervisionType.PAROLE)

    def test_get_supervision_type_probation_with_internal_unknown(self):
        # Arrange
        mo_supervision_sentence_1 = self._sup_sentence_with_type(StateSupervisionType.INTERNAL_UNKNOWN)
        mo_supervision_sentence_2 = self._sup_sentence_with_type(StateSupervisionType.PROBATION)

        # Act
        supervision_period_supervision_type = \
            us_mo_get_supervision_period_supervision_type_on_date(self.validation_date,
                                                                  supervision_sentences=[mo_supervision_sentence_1,
                                                                                         mo_supervision_sentence_2],
                                                                  incarceration_sentences=[])

        # Assert
        self.assertEqual(supervision_period_supervision_type, StateSupervisionPeriodSupervisionType.PROBATION)

    def test_get_supervision_type_internal_unknown(self):
        # Arrange
        mo_supervision_sentence = self._sup_sentence_with_type(StateSupervisionType.INTERNAL_UNKNOWN)

        # Act
        supervision_period_supervision_type = \
            us_mo_get_supervision_period_supervision_type_on_date(self.validation_date,
                                                                  supervision_sentences=[mo_supervision_sentence],
                                                                  incarceration_sentences=[])

        # Assert
        self.assertEqual(supervision_period_supervision_type, StateSupervisionPeriodSupervisionType.INTERNAL_UNKNOWN)


class UsMoGetMonthSupervisionTypeTest(unittest.TestCase):
    """Unittests for the us_mo_get_month_supervision_type helper."""

    def setUp(self) -> None:
        self.start_of_month_date = datetime.date(2019, 12, 1)
        self.end_of_month_date = datetime.date(2019, 12, 31)
        self.start_of_next_month_date = datetime.date(2020, 1, 1)

    def test_month_supervision_type_no_sentences(self):
        # Arrange
        supervision_period = StateSupervisionPeriod.new_with_defaults(
            supervision_period_id=1234,
            external_id='sp1',
            state_code='US_MO',
            start_date=(self.end_of_month_date - datetime.timedelta(days=60)),
            status=StateSupervisionPeriodStatus.PRESENT_WITHOUT_INFO
        )

        # Act
        supervision_period_supervision_type = \
            us_mo_get_month_supervision_type(self.end_of_month_date,
                                             supervision_sentences=[],
                                             incarceration_sentences=[],
                                             supervision_period=supervision_period)

        # Assert
        self.assertEqual(supervision_period_supervision_type,
                         StateSupervisionPeriodSupervisionType.INTERNAL_UNKNOWN)

    def test_month_supervision_type_single_sentence(self):
        # Arrange
        supervision_period = StateSupervisionPeriod.new_with_defaults(
            supervision_period_id=1234,
            external_id='sp1',
            state_code='US_MO',
            start_date=(self.end_of_month_date - datetime.timedelta(days=60)),
            status=StateSupervisionPeriodStatus.PRESENT_WITHOUT_INFO
        )

        supervision_sentence = FakeUsMoSupervisionSentence.fake_sentence_from_sentence(
            StateSupervisionSentence.new_with_defaults(
                external_id='ss1',
                state_code='US_MO',
                start_date=supervision_period.start_date
            ),
            supervision_type_spans=[
                SupervisionTypeSpan(start_date=supervision_period.start_date,
                                    end_date=None,
                                    supervision_type=StateSupervisionType.PROBATION)
            ])

        # Act
        supervision_period_supervision_type = \
            us_mo_get_month_supervision_type(self.end_of_month_date,
                                             supervision_sentences=[supervision_sentence],
                                             incarceration_sentences=[],
                                             supervision_period=supervision_period)

        # Assert
        self.assertEqual(supervision_period_supervision_type,
                         StateSupervisionPeriodSupervisionType.PROBATION)

    def test_month_supervision_type_no_supervision_all_month(self):
        # Arrange
        supervision_period = StateSupervisionPeriod.new_with_defaults(
            supervision_period_id=1234,
            external_id='sp1',
            state_code='US_MO',
            start_date=(self.end_of_month_date - datetime.timedelta(days=60)),
            status=StateSupervisionPeriodStatus.PRESENT_WITHOUT_INFO
        )

        supervision_sentence = FakeUsMoSupervisionSentence.fake_sentence_from_sentence(
            StateSupervisionSentence.new_with_defaults(
                external_id='ss1',
                state_code='US_MO',
                start_date=supervision_period.start_date
            ),
            supervision_type_spans=[
                SupervisionTypeSpan(start_date=supervision_period.start_date,
                                    end_date=self.start_of_month_date,
                                    supervision_type=StateSupervisionType.PROBATION),
                # Incarcerated / suspended since start of month
                SupervisionTypeSpan(start_date=self.start_of_month_date,
                                    end_date=None,
                                    supervision_type=None)
            ])

        # Act
        supervision_period_supervision_type = \
            us_mo_get_month_supervision_type(self.end_of_month_date,
                                             supervision_sentences=[supervision_sentence],
                                             incarceration_sentences=[],
                                             supervision_period=supervision_period)

        # Assert
        self.assertEqual(supervision_period_supervision_type,
                         StateSupervisionPeriodSupervisionType.INTERNAL_UNKNOWN)

    def test_month_supervision_type_supervision_ends_middle_of_month(self):
        # Arrange
        supervision_period = StateSupervisionPeriod.new_with_defaults(
            supervision_period_id=1234,
            external_id='sp1',
            state_code='US_MO',
            start_date=(self.end_of_month_date - datetime.timedelta(days=60)),
            status=StateSupervisionPeriodStatus.PRESENT_WITHOUT_INFO
        )

        supervision_end_date_middle_of_month = self.start_of_month_date + datetime.timedelta(days=5)
        supervision_sentence = FakeUsMoSupervisionSentence.fake_sentence_from_sentence(
            StateSupervisionSentence.new_with_defaults(
                external_id='ss1',
                state_code='US_MO',
                start_date=supervision_period.start_date
            ),
            supervision_type_spans=[
                SupervisionTypeSpan(start_date=supervision_period.start_date,
                                    end_date=supervision_end_date_middle_of_month,
                                    supervision_type=StateSupervisionType.PROBATION),
                # Incarcerated / suspended since middle of month
                SupervisionTypeSpan(start_date=supervision_end_date_middle_of_month,
                                    end_date=None,
                                    supervision_type=None)
            ])

        # Act
        supervision_period_supervision_type = \
            us_mo_get_month_supervision_type(self.end_of_month_date,
                                             supervision_sentences=[supervision_sentence],
                                             incarceration_sentences=[],
                                             supervision_period=supervision_period)

        # Assert
        self.assertEqual(supervision_period_supervision_type,
                         StateSupervisionPeriodSupervisionType.PROBATION)

    def test_get_month_supervision_type_parole_transitions_to_probation(self):
        # Arrange
        supervision_period = StateSupervisionPeriod.new_with_defaults(
            supervision_period_id=1234,
            external_id='sp1',
            state_code='US_MO',
            start_date=(self.end_of_month_date - datetime.timedelta(days=60)),
            status=StateSupervisionPeriodStatus.PRESENT_WITHOUT_INFO
        )

        parole_end_date_middle_of_month = self.start_of_month_date + datetime.timedelta(days=5)
        incarceration_sentence = FakeUsMoIncarcerationSentence.fake_sentence_from_sentence(
            StateIncarcerationSentence.new_with_defaults(
                external_id='ss1',
                state_code='US_MO',
                start_date=supervision_period.start_date,
            status=StateSentenceStatus.PRESENT_WITHOUT_INFO
            ),
            supervision_type_spans=[
                SupervisionTypeSpan(start_date=supervision_period.start_date,
                                    end_date=parole_end_date_middle_of_month,
                                    supervision_type=StateSupervisionType.PAROLE),
                # Parole finishes middle of month
                SupervisionTypeSpan(start_date=parole_end_date_middle_of_month,
                                    end_date=None,
                                    supervision_type=None)
            ])

        supervision_sentence = FakeUsMoSupervisionSentence.fake_sentence_from_sentence(
            StateSupervisionSentence.new_with_defaults(
                external_id='ss1',
                state_code='US_MO',
                start_date=parole_end_date_middle_of_month
            ),
            supervision_type_spans=[
                # Probation sentence starts after parole sentence ends
                SupervisionTypeSpan(start_date=parole_end_date_middle_of_month,
                                    end_date=None,
                                    supervision_type=StateSupervisionType.PROBATION)
            ])

        # Act
        supervision_period_supervision_type = \
            us_mo_get_month_supervision_type(self.end_of_month_date,
                                             supervision_sentences=[supervision_sentence],
                                             incarceration_sentences=[incarceration_sentence],
                                             supervision_period=supervision_period)

        # Assert
        self.assertEqual(supervision_period_supervision_type,
                         StateSupervisionPeriodSupervisionType.PROBATION)

    def test_get_month_supervision_type_dual(self):
        # Arrange
        supervision_period = StateSupervisionPeriod.new_with_defaults(
            supervision_period_id=1234,
            external_id='sp1',
            state_code='US_MO',
            start_date=(self.end_of_month_date - datetime.timedelta(days=60)),
            status=StateSupervisionPeriodStatus.PRESENT_WITHOUT_INFO
        )

        incarceration_sentence = FakeUsMoIncarcerationSentence.fake_sentence_from_sentence(
            StateIncarcerationSentence.new_with_defaults(
                external_id='ss1',
                state_code='US_MO',
                start_date=supervision_period.start_date,
                status=StateSentenceStatus.PRESENT_WITHOUT_INFO
            ),
            supervision_type_spans=[
                SupervisionTypeSpan(start_date=supervision_period.start_date,
                                    end_date=self.start_of_next_month_date,
                                    supervision_type=StateSupervisionType.PAROLE),
                SupervisionTypeSpan(start_date=self.start_of_next_month_date,
                                    end_date=None,
                                    supervision_type=None)
            ])

        supervision_sentence = FakeUsMoSupervisionSentence.fake_sentence_from_sentence(
            StateSupervisionSentence.new_with_defaults(
                external_id='ss1',
                state_code='US_MO',
                start_date=supervision_period.start_date
            ),
            supervision_type_spans=[
                SupervisionTypeSpan(start_date=supervision_period.start_date,
                                    end_date=self.start_of_next_month_date,
                                    supervision_type=StateSupervisionType.PROBATION),
                SupervisionTypeSpan(start_date=self.start_of_next_month_date,
                                    end_date=None,
                                    supervision_type=None)
            ])

        # Act
        supervision_period_supervision_type = \
            us_mo_get_month_supervision_type(self.end_of_month_date,
                                             supervision_sentences=[supervision_sentence],
                                             incarceration_sentences=[incarceration_sentence],
                                             supervision_period=supervision_period)

        # Assert
        self.assertEqual(supervision_period_supervision_type,
                         StateSupervisionPeriodSupervisionType.DUAL)

    def test_get_month_supervision_type_dual_ends_mid_month(self):
        # Arrange
        supervision_period = StateSupervisionPeriod.new_with_defaults(
            supervision_period_id=1234,
            external_id='sp1',
            state_code='US_MO',
            start_date=(self.end_of_month_date - datetime.timedelta(days=60)),
            status=StateSupervisionPeriodStatus.PRESENT_WITHOUT_INFO
        )

        second_to_last_month_date = self.end_of_month_date - datetime.timedelta(days=1)
        incarceration_sentence = FakeUsMoIncarcerationSentence.fake_sentence_from_sentence(
            StateIncarcerationSentence.new_with_defaults(
                external_id='ss1',
                state_code='US_MO',
                start_date=supervision_period.start_date,
                status=StateSentenceStatus.PRESENT_WITHOUT_INFO
            ),
            supervision_type_spans=[
                SupervisionTypeSpan(start_date=supervision_period.start_date,
                                    end_date=second_to_last_month_date,
                                    supervision_type=StateSupervisionType.PAROLE),
                SupervisionTypeSpan(start_date=second_to_last_month_date,
                                    end_date=None,
                                    supervision_type=None)
            ])

        supervision_sentence = FakeUsMoSupervisionSentence.fake_sentence_from_sentence(
            StateSupervisionSentence.new_with_defaults(
                external_id='ss1',
                state_code='US_MO',
                start_date=supervision_period.start_date
            ),
            supervision_type_spans=[
                # Probation sentence starts after parole sentence ends
                SupervisionTypeSpan(start_date=supervision_period.start_date,
                                    end_date=second_to_last_month_date,
                                    supervision_type=StateSupervisionType.PROBATION),
                SupervisionTypeSpan(start_date=second_to_last_month_date,
                                    end_date=None,
                                    supervision_type=None)
            ])

        # Act
        supervision_period_supervision_type = \
            us_mo_get_month_supervision_type(self.end_of_month_date,
                                             supervision_sentences=[supervision_sentence],
                                             incarceration_sentences=[incarceration_sentence],
                                             supervision_period=supervision_period)

        # Assert

        # Even though both sentences are terminated before the last day, we still return DUAL
        self.assertEqual(supervision_period_supervision_type,
                         StateSupervisionPeriodSupervisionType.DUAL)

    def test_get_month_supervision_type_sentence_supervision_ends_different_days(self):
        # Arrange
        supervision_period = StateSupervisionPeriod.new_with_defaults(
            supervision_period_id=1234,
            external_id='sp1',
            state_code='US_MO',
            start_date=(self.end_of_month_date - datetime.timedelta(days=60)),
            status=StateSupervisionPeriodStatus.PRESENT_WITHOUT_INFO
        )

        mid_month_date = self.end_of_month_date - datetime.timedelta(days=10)
        second_to_last_month_date = self.end_of_month_date - datetime.timedelta(days=1)
        incarceration_sentence = FakeUsMoIncarcerationSentence.fake_sentence_from_sentence(
            StateIncarcerationSentence.new_with_defaults(
                external_id='ss1',
                state_code='US_MO',
                start_date=supervision_period.start_date,
                status=StateSentenceStatus.PRESENT_WITHOUT_INFO
            ),
            supervision_type_spans=[
                SupervisionTypeSpan(start_date=supervision_period.start_date,
                                    end_date=second_to_last_month_date,
                                    supervision_type=StateSupervisionType.PAROLE),
                SupervisionTypeSpan(start_date=second_to_last_month_date,
                                    end_date=None,
                                    supervision_type=None)
            ])

        supervision_sentence = FakeUsMoSupervisionSentence.fake_sentence_from_sentence(
            StateSupervisionSentence.new_with_defaults(
                external_id='ss1',
                state_code='US_MO',
                start_date=supervision_period.start_date
            ),
            supervision_type_spans=[
                # Probation sentence starts after parole sentence ends
                SupervisionTypeSpan(start_date=supervision_period.start_date,
                                    end_date=mid_month_date,
                                    supervision_type=StateSupervisionType.PROBATION),
                SupervisionTypeSpan(start_date=mid_month_date,
                                    end_date=None,
                                    supervision_type=None)
            ])

        # Act
        supervision_period_supervision_type = \
            us_mo_get_month_supervision_type(self.end_of_month_date,
                                             supervision_sentences=[supervision_sentence],
                                             incarceration_sentences=[incarceration_sentence],
                                             supervision_period=supervision_period)

        # Assert

        # Since the probation sentence ends before the parole sentence, the last valid supervision type is PAROLE
        self.assertEqual(supervision_period_supervision_type,
                         StateSupervisionPeriodSupervisionType.PAROLE)


class UsMoGetMostRecentSupervisionPeriodSupervisionTypeBeforeUpperBoundDayTest(unittest.TestCase):
    """Unittests for the us_mo_get_most_recent_supervision_period_supervision_type_before_upper_bound_day helper."""

    def setUp(self) -> None:
        self.upper_bound_date = datetime.date(2018, 10, 10)

    def test_most_recent_supervision_type_no_sentences_no_bound(self):
        # Act
        supervision_period_supervision_type = \
            us_mo_get_most_recent_supervision_period_supervision_type_before_upper_bound_day(
                upper_bound_exclusive_date=self.upper_bound_date,
                lower_bound_inclusive_date=None,
                supervision_sentences=[],
                incarceration_sentences=[])

        # Assert
        self.assertEqual(supervision_period_supervision_type, None)

    def test_most_recent_supervision_type_no_sentences_same_day_bound(self):
        # Act
        supervision_period_supervision_type = \
            us_mo_get_most_recent_supervision_period_supervision_type_before_upper_bound_day(
                upper_bound_exclusive_date=self.upper_bound_date,
                lower_bound_inclusive_date=self.upper_bound_date,
                supervision_sentences=[],
                incarceration_sentences=[])

        # Assert
        self.assertEqual(supervision_period_supervision_type, None)

    def test_most_recent_supervision_type_two_sentences_same_transition_day_one_different(self):
        # Arrange

        start_date = self.upper_bound_date - datetime.timedelta(days=5)
        transition_date_1 = self.upper_bound_date - datetime.timedelta(days=3)
        transition_date_2 = self.upper_bound_date - datetime.timedelta(days=1)

        incarceration_sentence = FakeUsMoIncarcerationSentence.fake_sentence_from_sentence(
            StateIncarcerationSentence.new_with_defaults(
                external_id='ss1',
                state_code='US_MO',
                start_date=start_date,
                status=StateSentenceStatus.PRESENT_WITHOUT_INFO
            ),
            supervision_type_spans=[
                SupervisionTypeSpan(start_date=start_date,
                                    end_date=transition_date_2,
                                    supervision_type=StateSupervisionType.PAROLE),
                SupervisionTypeSpan(start_date=transition_date_2,
                                    end_date=None,
                                    supervision_type=None)
            ])

        supervision_sentence_1 = FakeUsMoSupervisionSentence.fake_sentence_from_sentence(
            StateSupervisionSentence.new_with_defaults(
                external_id='ss1',
                state_code='US_MO',
                start_date=start_date
            ),
            supervision_type_spans=[
                SupervisionTypeSpan(start_date=start_date,
                                    end_date=transition_date_1,
                                    supervision_type=StateSupervisionType.PROBATION),
                SupervisionTypeSpan(start_date=transition_date_1,
                                    end_date=None,
                                    supervision_type=None),
            ])

        supervision_sentence_2 = FakeUsMoSupervisionSentence.fake_sentence_from_sentence(
            StateSupervisionSentence.new_with_defaults(
                external_id='ss1',
                state_code='US_MO',
                start_date=start_date
            ),
            supervision_type_spans=[
                SupervisionTypeSpan(start_date=start_date,
                                    end_date=transition_date_2,
                                    supervision_type=StateSupervisionType.PROBATION),
                SupervisionTypeSpan(start_date=transition_date_2,
                                    end_date=None,
                                    supervision_type=StateSupervisionType.PAROLE),
            ])

        # Act
        supervision_period_supervision_type = \
            us_mo_get_most_recent_supervision_period_supervision_type_before_upper_bound_day(
                upper_bound_exclusive_date=self.upper_bound_date,
                lower_bound_inclusive_date=None,
                supervision_sentences=[supervision_sentence_1, supervision_sentence_2],
                incarceration_sentences=[incarceration_sentence])

        # Assert

        # Since the probation sentence ends before the parole sentence, the last valid supervision type is PAROLE
        self.assertEqual(supervision_period_supervision_type,
                         StateSupervisionPeriodSupervisionType.PAROLE)
