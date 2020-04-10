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

from recidiviz.calculator.pipeline.utils.us_mo_sentence_classification import UsMoIncarcerationSentence, \
    UsMoSupervisionSentence
from recidiviz.calculator.pipeline.utils.us_mo_supervision_type_identification import \
    us_mo_get_supervision_period_supervision_type_on_date, us_mo_get_pre_incarceration_supervision_type
from recidiviz.common.constants.state.state_incarceration_period import StateIncarcerationPeriodAdmissionReason
from recidiviz.common.constants.state.state_sentence import StateSentenceStatus
from recidiviz.common.constants.state.state_supervision import StateSupervisionType
from recidiviz.common.constants.state.state_supervision_period import StateSupervisionPeriodSupervisionType
from recidiviz.persistence.entity.state.entities import StateIncarcerationSentence, StateSupervisionSentence, \
    StateIncarcerationPeriod
from recidiviz.tests.calculator.pipeline.utils.us_mo_fakes import FakeUsMoSupervisionSentence, \
    FakeUsMoIncarcerationSentence


class UsMoGetPreIncarcerationSupervisionTypeTest(unittest.TestCase):
    """Tests for us_mo_get_pre_incarceration_supervision_type"""

    REVOCATION_DATE = datetime.date(year=2019, month=9, day=13)
    REVOCATION_DATE_STR = REVOCATION_DATE.strftime('%Y%m%d')

    TERMINATED_BEFORE_REVOCATION_DATE_STATUSES = [
        {'sentence_external_id': '13252-20160627-1', 'sentence_status_external_id': '13252-20160627-1-1',
         'status_code': '10I1000', 'status_date': '20171012', 'status_description': 'New Court Comm-Institution'},
        {'sentence_external_id': '13252-20160627-1', 'sentence_status_external_id': '13252-20160627-1-2',
         'status_code': '90O1010', 'status_date': '20180912',
         'status_description': "Inst. Expiration of Sentence"},
    ]

    PAROLE_SENTENCE_EXTERNAL_ID = '1167633-20171012-2'

    def test_usMo_getPreIncarcerationSupervisionType(self):
        incarceration_period = StateIncarcerationPeriod.new_with_defaults(
            incarceration_period_id=1,
            admission_reason=StateIncarcerationPeriodAdmissionReason.PAROLE_REVOCATION,
            external_id='ip1',
            state_code='US_MO',
            admission_date=self.REVOCATION_DATE)

        supervision_sentence_parole = FakeUsMoSupervisionSentence.fake_sentence_from_sentence(
            StateSupervisionSentence.new_with_defaults(
                supervision_sentence_id=1,
                external_id=self.PAROLE_SENTENCE_EXTERNAL_ID,
                start_date=datetime.date(2017, 2, 1),
                supervision_type=StateSupervisionType.PROBATION),
            supervision_type=StateSupervisionType.PAROLE
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
            admission_date=self.REVOCATION_DATE)

        supervision_sentence_parole = FakeUsMoSupervisionSentence.fake_sentence_from_sentence(
            StateSupervisionSentence.new_with_defaults(
                supervision_sentence_id=1,
                external_id=self.PAROLE_SENTENCE_EXTERNAL_ID,
                start_date=datetime.date(2017, 2, 1),
                supervision_type=StateSupervisionType.PROBATION),
            supervision_type=StateSupervisionType.PAROLE
        )

        old_incarceration_sentence = FakeUsMoIncarcerationSentence.fake_sentence_from_sentence(
            StateIncarcerationSentence.new_with_defaults(
                incarceration_sentence_id=1,
                external_id='US_MO',
                start_date=datetime.date(2017, 2, 1),
                completion_date=datetime.date(2017, 3, 4),
                status=StateSentenceStatus.COMPLETED),
            supervision_type=None  # Terminated already
        )

        self.assertEqual(StateSupervisionPeriodSupervisionType.PAROLE,
                         us_mo_get_pre_incarceration_supervision_type(
                             [old_incarceration_sentence],
                             [supervision_sentence_parole],
                             incarceration_period))


class UsMoGetSupervisionPeriodSupervisionTypeOnDateTest(unittest.TestCase):
    """Unittests for the us_mo_get_supervision_period_supervision_type_on_date helper."""
    def setUp(self) -> None:
        self.validation_date = datetime.date(year=2019, month=10, day=31)
        self.sentence_id_counter = 0

    def _inc_sentence_with_type(self, supervision_type: Optional[StateSupervisionType]) -> UsMoIncarcerationSentence:
        base_sentence = StateIncarcerationSentence.new_with_defaults(
            external_id=f'164735-20120627-{self.sentence_id_counter}',
            state_code='US_MO',
            start_date=datetime.date(year=2012, month=6, day=27)
        )
        mo_sentence = FakeUsMoIncarcerationSentence.fake_sentence_from_sentence(
            base_sentence, supervision_type=supervision_type)

        self.sentence_id_counter += 1

        return mo_sentence

    def _sup_sentence_with_type(self, supervision_type: Optional[StateSupervisionType]) -> UsMoSupervisionSentence:
        base_sentence = StateSupervisionSentence.new_with_defaults(
            external_id=f'164735-20120627-{self.sentence_id_counter}',
            state_code='US_MO',
            start_date=datetime.date(year=2012, month=6, day=27)
        )
        mo_sentence = FakeUsMoSupervisionSentence.fake_sentence_from_sentence(
            base_sentence, supervision_type=supervision_type)

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
