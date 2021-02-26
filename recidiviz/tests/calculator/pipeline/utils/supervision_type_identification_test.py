# Recidiviz - a data platform for criminal justice reform
# Copyright (C) 2019 Recidiviz, Inc.
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
# pylint: disable=unused-import,wrong-import-order

"""Tests for supervision_type_identification.py."""

import unittest
from datetime import date


from recidiviz.calculator.pipeline.utils.supervision_type_identification import \
    _get_sentences_overlapping_with_date, _get_sentences_overlapping_with_dates, _get_valid_attached_sentences, \
    _get_sentence_supervision_type_from_sentence, get_pre_incarceration_supervision_type_from_incarceration_period
from recidiviz.calculator.pipeline.utils.state_utils.state_calculation_config_manager import get_month_supervision_type
from recidiviz.common.constants.state.state_incarceration_period import StateIncarcerationPeriodAdmissionReason, \
    StateIncarcerationPeriodStatus
from recidiviz.common.constants.state.state_sentence import StateSentenceStatus
from recidiviz.common.constants.state.state_supervision import StateSupervisionType
from recidiviz.common.constants.state.state_supervision_period import StateSupervisionPeriodSupervisionType, \
    StateSupervisionPeriodStatus, StateSupervisionPeriodTerminationReason
from recidiviz.persistence.entity.state.entities import \
    StateIncarcerationPeriod, StateSupervisionPeriod, StateSupervisionSentence, StateIncarcerationSentence


class TestGetMonthSupervisionType(unittest.TestCase):
    """Tests the get_month_supervision_type function."""
    def test_get_month_supervision_type_dual(self):
        any_date_in_month = date(2018, 4, 13)

        supervision_period = \
            StateSupervisionPeriod.new_with_defaults(
                supervision_period_id=111,
                external_id='sp1',
                status=StateSupervisionPeriodStatus.TERMINATED,
                state_code='US_XX',
                start_date=date(2018, 3, 5),
                termination_date=date(2018, 5, 19),
                termination_reason=StateSupervisionPeriodTerminationReason.DISCHARGE,
                supervision_type=None
            )

        supervision_sentence = \
            StateSupervisionSentence.new_with_defaults(
                state_code='US_XX',
                supervision_sentence_id=111,
                external_id='ss1',
                start_date=date(2018, 3, 1),
                completion_date=date(2018, 5, 30),
                status=StateSentenceStatus.COMPLETED,
                projected_completion_date=date(2018, 5, 19),
                supervision_type=StateSupervisionType.PROBATION,
                supervision_periods=[supervision_period]
            )

        incarceration_sentence = StateIncarcerationSentence.new_with_defaults(
            state_code='US_XX',
            incarceration_sentence_id=123,
            external_id='is1',
            start_date=date(2018, 3, 1),
            completion_date=date(2018, 5, 30),
            supervision_periods=[supervision_period],
            status=StateSentenceStatus.PRESENT_WITHOUT_INFO
        )

        supervision_sentences = [supervision_sentence]
        incarceration_sentences = [incarceration_sentence]

        supervision_type = get_month_supervision_type(
            any_date_in_month, supervision_sentences, incarceration_sentences, supervision_period)

        self.assertEqual(StateSupervisionPeriodSupervisionType.DUAL, supervision_type)

    def test_get_month_supervision_type_probation(self):
        any_date_in_month = date(2018, 4, 13)

        supervision_period = \
            StateSupervisionPeriod.new_with_defaults(
                supervision_period_id=111,
                external_id='sp1',
                status=StateSupervisionPeriodStatus.TERMINATED,
                state_code='US_XX',
                start_date=date(2018, 3, 5),
                termination_date=date(2018, 5, 19),
                termination_reason=StateSupervisionPeriodTerminationReason.DISCHARGE,
                supervision_type=None
            )

        supervision_sentence = \
            StateSupervisionSentence.new_with_defaults(
                state_code='US_XX',
                supervision_sentence_id=111,
                external_id='ss1',
                start_date=date(2018, 3, 1),
                completion_date=date(2018, 5, 30),
                status=StateSentenceStatus.COMPLETED,
                projected_completion_date=date(2018, 5, 19),
                supervision_type=StateSupervisionType.PROBATION,
                supervision_periods=[supervision_period]
            )

        supervision_sentences = [supervision_sentence]
        incarceration_sentences = []

        supervision_type = get_month_supervision_type(
            any_date_in_month, supervision_sentences, incarceration_sentences, supervision_period)

        self.assertEqual(StateSupervisionPeriodSupervisionType.PROBATION, supervision_type)

    def test_get_month_supervision_type_parole(self):
        any_date_in_month = date(2018, 4, 13)

        supervision_period = \
            StateSupervisionPeriod.new_with_defaults(
                supervision_period_id=111,
                external_id='sp1',
                status=StateSupervisionPeriodStatus.TERMINATED,
                state_code='US_XX',
                start_date=date(2018, 3, 5),
                termination_date=date(2018, 5, 19),
                termination_reason=StateSupervisionPeriodTerminationReason.DISCHARGE,
                supervision_type=None
            )

        incarceration_sentence = StateIncarcerationSentence.new_with_defaults(
            state_code='US_XX',
            incarceration_sentence_id=123,
            external_id='is1',
            start_date=date(2018, 3, 1),
            completion_date=date(2018, 5, 30),
            supervision_periods=[supervision_period],
            status=StateSentenceStatus.PRESENT_WITHOUT_INFO
        )

        supervision_sentences = []
        incarceration_sentences = [incarceration_sentence]

        supervision_type = get_month_supervision_type(
            any_date_in_month, supervision_sentences, incarceration_sentences, supervision_period)

        self.assertEqual(StateSupervisionPeriodSupervisionType.PAROLE, supervision_type)

    def test_get_month_supervision_type_incarceration_sentence_and_no_type_set_on_supervision_sentence(self):
        any_date_in_month = date(2018, 4, 13)

        supervision_period = \
            StateSupervisionPeriod.new_with_defaults(
                supervision_period_id=111,
                external_id='sp1',
                status=StateSupervisionPeriodStatus.TERMINATED,
                state_code='US_XX',
                start_date=date(2018, 3, 5),
                termination_date=date(2018, 5, 19),
                termination_reason=StateSupervisionPeriodTerminationReason.DISCHARGE,
                supervision_type=None
            )

        supervision_sentence = \
            StateSupervisionSentence.new_with_defaults(
                state_code='US_XX',
                supervision_sentence_id=111,
                external_id='ss1',
                start_date=date(2018, 3, 1),
                completion_date=date(2018, 5, 30),
                status=StateSentenceStatus.COMPLETED,
                projected_completion_date=date(2018, 5, 19),
                supervision_periods=[supervision_period]
            )

        incarceration_sentence = StateIncarcerationSentence.new_with_defaults(
            state_code='US_XX',
            incarceration_sentence_id=123,
            external_id='is1',
            start_date=date(2018, 3, 1),
            completion_date=date(2018, 5, 30),
            supervision_periods=[supervision_period],
            status=StateSentenceStatus.PRESENT_WITHOUT_INFO
        )

        supervision_sentences = [supervision_sentence]
        incarceration_sentences = [incarceration_sentence]

        supervision_type = get_month_supervision_type(
            any_date_in_month, supervision_sentences, incarceration_sentences, supervision_period)

        self.assertEqual(StateSupervisionPeriodSupervisionType.DUAL, supervision_type)

    def test_get_month_supervision_type_no_type_set_on_supervision_sentence(self):
        any_date_in_month = date(2018, 4, 13)

        supervision_period = \
            StateSupervisionPeriod.new_with_defaults(
                supervision_period_id=111,
                external_id='sp1',
                status=StateSupervisionPeriodStatus.TERMINATED,
                state_code='US_XX',
                start_date=date(2018, 3, 5),
                termination_date=date(2018, 5, 19),
                termination_reason=StateSupervisionPeriodTerminationReason.DISCHARGE,
                supervision_type=None
            )

        supervision_sentence = \
            StateSupervisionSentence.new_with_defaults(
                state_code='US_XX',
                supervision_sentence_id=111,
                external_id='ss1',
                start_date=date(2018, 3, 1),
                completion_date=date(2018, 5, 30),
                status=StateSentenceStatus.COMPLETED,
                projected_completion_date=date(2018, 5, 19),
                supervision_periods=[supervision_period]
            )

        supervision_sentences = [supervision_sentence]
        incarceration_sentences = []

        supervision_type = get_month_supervision_type(
            any_date_in_month, supervision_sentences, incarceration_sentences, supervision_period)

        self.assertEqual(StateSupervisionPeriodSupervisionType.PROBATION, supervision_type)

    def test_get_month_supervision_type_internal_unknown_on_supervision_sentence(self):
        any_date_in_month = date(2018, 4, 13)

        supervision_period = \
            StateSupervisionPeriod.new_with_defaults(
                supervision_period_id=111,
                external_id='sp1',
                status=StateSupervisionPeriodStatus.TERMINATED,
                state_code='US_XX',
                start_date=date(2018, 3, 5),
                termination_date=date(2018, 5, 19),
                termination_reason=StateSupervisionPeriodTerminationReason.DISCHARGE,
                supervision_type=None
            )

        supervision_sentence = \
            StateSupervisionSentence.new_with_defaults(
                state_code='US_XX',
                supervision_sentence_id=111,
                external_id='ss1',
                start_date=date(2018, 3, 1),
                completion_date=date(2018, 5, 30),
                status=StateSentenceStatus.COMPLETED,
                projected_completion_date=date(2018, 5, 19),
                supervision_type=StateSupervisionType.INTERNAL_UNKNOWN,
                supervision_periods=[supervision_period]
            )

        supervision_sentences = [supervision_sentence]
        incarceration_sentences = []

        supervision_type = get_month_supervision_type(
            any_date_in_month, supervision_sentences, incarceration_sentences, supervision_period)

        self.assertEqual(StateSupervisionPeriodSupervisionType.INTERNAL_UNKNOWN, supervision_type)

    def test_get_month_supervision_type_no_sentences_internal_unknown(self):
        any_date_in_month = date(2018, 4, 13)

        supervision_period = \
            StateSupervisionPeriod.new_with_defaults(
                supervision_period_id=111,
                external_id='sp1',
                status=StateSupervisionPeriodStatus.TERMINATED,
                state_code='US_XX',
                start_date=date(2018, 3, 5),
                termination_date=date(2018, 5, 19),
                termination_reason=StateSupervisionPeriodTerminationReason.DISCHARGE,
                supervision_type=None
            )

        supervision_sentences = []
        incarceration_sentences = []

        supervision_type = get_month_supervision_type(
            any_date_in_month, supervision_sentences, incarceration_sentences, supervision_period)

        self.assertEqual(StateSupervisionPeriodSupervisionType.INTERNAL_UNKNOWN, supervision_type)

    def test_get_month_supervision_type_probation_options(self):
        any_date_in_month = date(2018, 4, 13)

        supervision_period = \
            StateSupervisionPeriod.new_with_defaults(
                supervision_period_id=111,
                external_id='sp1',
                status=StateSupervisionPeriodStatus.TERMINATED,
                state_code='US_XX',
                start_date=date(2018, 3, 5),
                termination_date=date(2018, 5, 19),
                termination_reason=StateSupervisionPeriodTerminationReason.DISCHARGE,
                supervision_type=None
            )

        supervision_sentence = \
            StateSupervisionSentence.new_with_defaults(
                state_code='US_XX',
                supervision_sentence_id=111,
                external_id='ss1',
                start_date=date(2018, 3, 1),
                completion_date=date(2018, 5, 30),
                status=StateSentenceStatus.COMPLETED,
                projected_completion_date=date(2018, 5, 19),
                supervision_periods=[supervision_period]
            )

        supervision_sentences = [supervision_sentence]
        incarceration_sentences = []

        probation_types = [
            StateSupervisionType.PRE_CONFINEMENT,
            StateSupervisionType.POST_CONFINEMENT,
            StateSupervisionType.HALFWAY_HOUSE,
            StateSupervisionType.CIVIL_COMMITMENT
        ]

        for sup_type in probation_types:
            supervision_sentence.supervision_type = sup_type

            supervision_type = get_month_supervision_type(
                any_date_in_month, supervision_sentences, incarceration_sentences, supervision_period)

            self.assertEqual(StateSupervisionPeriodSupervisionType.PROBATION, supervision_type)

    def test_get_month_supervision_type_test_all_enums(self):
        any_date_in_month = date(2018, 4, 13)

        supervision_period = \
            StateSupervisionPeriod.new_with_defaults(
                supervision_period_id=111,
                external_id='sp1',
                status=StateSupervisionPeriodStatus.TERMINATED,
                state_code='US_XX',
                start_date=date(2018, 3, 5),
                termination_date=date(2018, 5, 19),
                termination_reason=StateSupervisionPeriodTerminationReason.DISCHARGE,
                supervision_type=None
            )

        supervision_sentence = \
            StateSupervisionSentence.new_with_defaults(
                state_code='US_XX',
                supervision_sentence_id=111,
                external_id='ss1',
                start_date=date(2018, 3, 1),
                completion_date=date(2018, 5, 30),
                status=StateSentenceStatus.COMPLETED,
                projected_completion_date=date(2018, 5, 19),
                supervision_type=StateSupervisionType.INTERNAL_UNKNOWN,
                supervision_periods=[supervision_period]
            )

        supervision_sentences = [supervision_sentence]
        incarceration_sentences = []

        for sup_type in StateSupervisionType:
            supervision_sentence.supervision_type = sup_type

            # Tests that all cases are covered
            _ = get_month_supervision_type(
                any_date_in_month, supervision_sentences, incarceration_sentences, supervision_period)

    def test_get_month_supervision_type_no_start_date_on_sentence_drop_it(self):
        any_date_in_month = date(2018, 4, 13)

        supervision_period = \
            StateSupervisionPeriod.new_with_defaults(
                supervision_period_id=111,
                external_id='sp1',
                status=StateSupervisionPeriodStatus.TERMINATED,
                state_code='US_XX',
                start_date=date(2018, 3, 5),
                termination_date=date(2018, 5, 19),
                termination_reason=StateSupervisionPeriodTerminationReason.DISCHARGE,
                supervision_type=None
            )

        supervision_sentence = \
            StateSupervisionSentence.new_with_defaults(
                state_code='US_XX',
                supervision_sentence_id=111,
                external_id='ss1',
                completion_date=date(2018, 5, 30),
                status=StateSentenceStatus.COMPLETED,
                supervision_type=StateSupervisionType.PROBATION,
                projected_completion_date=date(2018, 5, 19),
                supervision_periods=[supervision_period]
            )

        supervision_sentences = [supervision_sentence]
        incarceration_sentences = []

        supervision_type = get_month_supervision_type(
            any_date_in_month, supervision_sentences, incarceration_sentences, supervision_period)

        self.assertEqual(StateSupervisionPeriodSupervisionType.INTERNAL_UNKNOWN, supervision_type)

    def test_get_month_supervision_type_no_completion_date_on_sentence(self):
        any_date_in_month = date(2018, 4, 13)

        supervision_period = \
            StateSupervisionPeriod.new_with_defaults(
                supervision_period_id=111,
                external_id='sp1',
                status=StateSupervisionPeriodStatus.TERMINATED,
                state_code='US_XX',
                start_date=date(2018, 3, 5),
                termination_date=date(2018, 5, 19),
                termination_reason=StateSupervisionPeriodTerminationReason.DISCHARGE,
                supervision_type=None
            )

        supervision_sentence = \
            StateSupervisionSentence.new_with_defaults(
                state_code='US_XX',
                supervision_sentence_id=111,
                external_id='ss1',
                start_date=date(2018, 2, 28),
                status=StateSentenceStatus.COMPLETED,
                supervision_type=StateSupervisionType.PROBATION,
                projected_completion_date=date(2018, 5, 19),
                supervision_periods=[supervision_period]
            )

        supervision_sentences = [supervision_sentence]
        incarceration_sentences = []

        supervision_type = get_month_supervision_type(
            any_date_in_month, supervision_sentences, incarceration_sentences, supervision_period)

        self.assertEqual(StateSupervisionPeriodSupervisionType.PROBATION, supervision_type)

    def test_get_month_supervision_type_no_dates_on_sentence(self):
        any_date_in_month = date(2018, 4, 13)

        supervision_period = \
            StateSupervisionPeriod.new_with_defaults(
                supervision_period_id=111,
                external_id='sp1',
                status=StateSupervisionPeriodStatus.TERMINATED,
                state_code='US_XX',
                start_date=date(2018, 3, 5),
                termination_date=date(2018, 5, 19),
                termination_reason=StateSupervisionPeriodTerminationReason.DISCHARGE,
                supervision_type=None
            )

        supervision_sentence = \
            StateSupervisionSentence.new_with_defaults(
                state_code='US_XX',
                supervision_sentence_id=111,
                external_id='ss1',
                status=StateSentenceStatus.COMPLETED,
                supervision_periods=[supervision_period]
            )

        supervision_sentences = [supervision_sentence]
        incarceration_sentences = []

        supervision_type = get_month_supervision_type(
            any_date_in_month, supervision_sentences, incarceration_sentences, supervision_period)

        self.assertEqual(StateSupervisionPeriodSupervisionType.INTERNAL_UNKNOWN, supervision_type)

    def test_get_month_supervision_type_completed_on_end_of_month(self):
        any_date_in_month = date(2018, 4, 13)

        supervision_period = \
            StateSupervisionPeriod.new_with_defaults(
                supervision_period_id=111,
                external_id='sp1',
                status=StateSupervisionPeriodStatus.TERMINATED,
                state_code='US_XX',
                start_date=date(2018, 3, 5),
                termination_date=date(2018, 5, 19),
                termination_reason=StateSupervisionPeriodTerminationReason.DISCHARGE,
                supervision_type=None
            )

        supervision_sentence = \
            StateSupervisionSentence.new_with_defaults(
                state_code='US_XX',
                supervision_sentence_id=111,
                external_id='ss1',
                start_date=date(2018, 2, 28),
                completion_date=date(2018, 4, 30),
                status=StateSentenceStatus.COMPLETED,
                supervision_type=StateSupervisionType.PROBATION,
                projected_completion_date=date(2018, 5, 19),
                supervision_periods=[supervision_period]
            )

        supervision_sentences = [supervision_sentence]
        incarceration_sentences = []

        supervision_type = get_month_supervision_type(
            any_date_in_month, supervision_sentences, incarceration_sentences, supervision_period)

        self.assertEqual(StateSupervisionPeriodSupervisionType.PROBATION, supervision_type)

    def test_get_month_supervision_type_started_on_end_of_month(self):
        any_date_in_month = date(2018, 4, 13)

        supervision_period = \
            StateSupervisionPeriod.new_with_defaults(
                supervision_period_id=111,
                external_id='sp1',
                status=StateSupervisionPeriodStatus.TERMINATED,
                state_code='US_XX',
                start_date=date(2018, 3, 5),
                termination_date=date(2018, 5, 19),
                termination_reason=StateSupervisionPeriodTerminationReason.DISCHARGE,
                supervision_type=None
            )

        supervision_sentence = \
            StateSupervisionSentence.new_with_defaults(
                state_code='US_XX',
                supervision_sentence_id=111,
                external_id='ss1',
                start_date=date(2018, 4, 30),
                status=StateSentenceStatus.COMPLETED,
                supervision_type=StateSupervisionType.PROBATION,
                projected_completion_date=date(2018, 5, 19),
                supervision_periods=[supervision_period]
            )

        supervision_sentences = [supervision_sentence]
        incarceration_sentences = []

        supervision_type = get_month_supervision_type(
            any_date_in_month, supervision_sentences, incarceration_sentences, supervision_period)

        self.assertEqual(StateSupervisionPeriodSupervisionType.PROBATION, supervision_type)

    def test_get_month_supervision_type_completed_on_first_of_month(self):
        any_date_in_month = date(2018, 4, 13)

        supervision_period = \
            StateSupervisionPeriod.new_with_defaults(
                supervision_period_id=111,
                external_id='sp1',
                status=StateSupervisionPeriodStatus.TERMINATED,
                state_code='US_XX',
                start_date=date(2018, 3, 5),
                termination_date=date(2018, 5, 19),
                termination_reason=StateSupervisionPeriodTerminationReason.DISCHARGE,
                supervision_type=None
            )

        supervision_sentence = \
            StateSupervisionSentence.new_with_defaults(
                state_code='US_XX',
                supervision_sentence_id=111,
                external_id='ss1',
                start_date=date(2018, 2, 28),
                completion_date=date(2018, 4, 1),
                status=StateSentenceStatus.COMPLETED,
                supervision_type=StateSupervisionType.PAROLE,
                projected_completion_date=date(2018, 5, 19),
                supervision_periods=[supervision_period]
            )

        supervision_sentences = [supervision_sentence]
        incarceration_sentences = []

        supervision_type = get_month_supervision_type(
            any_date_in_month, supervision_sentences, incarceration_sentences, supervision_period)

        self.assertEqual(StateSupervisionPeriodSupervisionType.PAROLE, supervision_type)

    def test_get_month_supervision_type_started_and_completed_on_first_of_month(self):
        any_date_in_month = date(2018, 4, 13)

        supervision_period = \
            StateSupervisionPeriod.new_with_defaults(
                supervision_period_id=111,
                external_id='sp1',
                status=StateSupervisionPeriodStatus.TERMINATED,
                state_code='US_XX',
                start_date=date(2018, 3, 5),
                termination_date=date(2018, 5, 19),
                termination_reason=StateSupervisionPeriodTerminationReason.DISCHARGE,
                supervision_type=None
            )

        supervision_sentence = \
            StateSupervisionSentence.new_with_defaults(
                state_code='US_XX',
                supervision_sentence_id=111,
                external_id='ss1',
                start_date=date(2018, 4, 1),
                completion_date=date(2018, 4, 1),
                status=StateSentenceStatus.COMPLETED,
                supervision_type=StateSupervisionType.PAROLE,
                projected_completion_date=date(2018, 5, 19),
                supervision_periods=[supervision_period]
            )

        supervision_sentences = [supervision_sentence]
        incarceration_sentences = []

        supervision_type = get_month_supervision_type(
            any_date_in_month, supervision_sentences, incarceration_sentences, supervision_period)

        self.assertEqual(StateSupervisionPeriodSupervisionType.PAROLE, supervision_type)

    def test_get_month_supervision_type_started_and_completed_on_end_of_month(self):
        any_date_in_month = date(2018, 4, 13)

        supervision_period = \
            StateSupervisionPeriod.new_with_defaults(
                supervision_period_id=111,
                external_id='sp1',
                status=StateSupervisionPeriodStatus.TERMINATED,
                state_code='US_XX',
                start_date=date(2018, 3, 5),
                termination_date=date(2018, 5, 19),
                termination_reason=StateSupervisionPeriodTerminationReason.DISCHARGE,
                supervision_type=None
            )

        supervision_sentence = \
            StateSupervisionSentence.new_with_defaults(
                state_code='US_XX',
                supervision_sentence_id=111,
                external_id='ss1',
                start_date=date(2018, 4, 30),
                completion_date=date(2018, 4, 30),
                status=StateSentenceStatus.COMPLETED,
                supervision_type=StateSupervisionType.PAROLE,
                projected_completion_date=date(2018, 5, 19),
                supervision_periods=[supervision_period]
            )

        supervision_sentences = [supervision_sentence]
        incarceration_sentences = []

        supervision_type = get_month_supervision_type(
            any_date_in_month, supervision_sentences, incarceration_sentences, supervision_period)

        self.assertEqual(StateSupervisionPeriodSupervisionType.PAROLE, supervision_type)

    def test_get_month_supervision_type_started_after_month_ended(self):
        any_date_in_month = date(2018, 4, 13)

        supervision_period = \
            StateSupervisionPeriod.new_with_defaults(
                supervision_period_id=111,
                external_id='sp1',
                status=StateSupervisionPeriodStatus.TERMINATED,
                state_code='US_XX',
                start_date=date(2018, 3, 5),
                termination_date=date(2018, 5, 19),
                termination_reason=StateSupervisionPeriodTerminationReason.DISCHARGE,
                supervision_type=None
            )

        supervision_sentence = \
            StateSupervisionSentence.new_with_defaults(
                state_code='US_XX',
                supervision_sentence_id=111,
                external_id='ss1',
                start_date=date(2018, 5, 1),
                status=StateSentenceStatus.COMPLETED,
                supervision_type=StateSupervisionType.PROBATION,
                projected_completion_date=date(2018, 5, 19),
                supervision_periods=[supervision_period]
            )

        supervision_sentences = [supervision_sentence]
        incarceration_sentences = []

        supervision_type = get_month_supervision_type(
            any_date_in_month, supervision_sentences, incarceration_sentences, supervision_period)

        self.assertEqual(StateSupervisionPeriodSupervisionType.INTERNAL_UNKNOWN, supervision_type)


class TestGetPreIncarcerationSupervisionType(unittest.TestCase):
    """Tests get_pre_incarceration_supervision_type_from_incarceration_period."""

    def test_getPreIncarcerationSupervisionType_typeBasedOnAdmissionReason(self):
        for admission_reason in StateIncarcerationPeriodAdmissionReason:
            incarceration_period = StateIncarcerationPeriod.new_with_defaults(
                admission_reason=admission_reason,
                state_code='US_XX',
                status=StateIncarcerationPeriodStatus.PRESENT_WITHOUT_INFO)
            expected_type = None
            if admission_reason == StateIncarcerationPeriodAdmissionReason.PROBATION_REVOCATION:
                expected_type = StateSupervisionPeriodSupervisionType.PROBATION
            elif admission_reason == StateIncarcerationPeriodAdmissionReason.PAROLE_REVOCATION:
                expected_type = StateSupervisionPeriodSupervisionType.PAROLE
            elif admission_reason == StateIncarcerationPeriodAdmissionReason.DUAL_REVOCATION:
                expected_type = StateSupervisionPeriodSupervisionType.DUAL
            self.assertEqual(expected_type,
                             get_pre_incarceration_supervision_type_from_incarceration_period(incarceration_period))

    def test_getSentencesOverlappingWithDate(self):
        target_date = date(2018, 7, 20)
        valid_incarceration_sentence = StateIncarcerationSentence.new_with_defaults(
            state_code='US_XX',
            incarceration_sentence_id=1,
            external_id='is1',
            start_date=date(2018, 7, 1),
            completion_date=date(2018, 7, 30),
            status=StateSentenceStatus.PRESENT_WITHOUT_INFO)
        valid_incarceration_sentence_2 = StateIncarcerationSentence.new_with_defaults(
            state_code='US_XX',
            incarceration_sentence_id=1,
            external_id='is1',
            start_date=date(2018, 7, 1),
            completion_date=date(2018, 7, 20),
            status=StateSentenceStatus.PRESENT_WITHOUT_INFO)
        invalid_incarceration_sentence_2 = StateIncarcerationSentence.new_with_defaults(
            state_code='US_XX',
            incarceration_sentence_id=1,
            external_id='is1',
            start_date=date(2018, 7, 21),
            completion_date=date(2018, 8, 20),
            status=StateSentenceStatus.PRESENT_WITHOUT_INFO)

        self.assertEqual([valid_incarceration_sentence, valid_incarceration_sentence_2],
                         _get_sentences_overlapping_with_date(target_date, [valid_incarceration_sentence,
                                                                            valid_incarceration_sentence_2,
                                                                            invalid_incarceration_sentence_2]))

    def test_getValidAttachedSentences(self):
        supervision_period = StateSupervisionPeriod.new_with_defaults(
            state_code='US_XX', supervision_period_id=1, status=StateSupervisionPeriodStatus.PRESENT_WITHOUT_INFO)
        valid_incarceration_sentence = StateIncarcerationSentence.new_with_defaults(
            state_code='US_XX',
            incarceration_sentence_id=1,
            external_id='is1',
            start_date=date(2018, 7, 1),
            supervision_periods=[supervision_period],
            status=StateSentenceStatus.PRESENT_WITHOUT_INFO)

        invalid_incarceration_sentence = StateIncarcerationSentence.new_with_defaults(
            state_code='US_XX',
            incarceration_sentence_id=1,
            external_id='is1',
            supervision_periods=[supervision_period],
            status=StateSentenceStatus.PRESENT_WITHOUT_INFO)
        invalid_incarceration_sentence_2 = StateIncarcerationSentence.new_with_defaults(
            state_code='US_XX',
            incarceration_sentence_id=1,
            start_date=date(2018, 7, 1),
            external_id='is1',
            status=StateSentenceStatus.PRESENT_WITHOUT_INFO)
        sentences = [valid_incarceration_sentence, invalid_incarceration_sentence, invalid_incarceration_sentence_2]

        self.assertEqual([valid_incarceration_sentence],
                         _get_valid_attached_sentences(sentences, supervision_period))

    def test_getSentencesOverlappingWithDates(self):

        start_date = date(2018, 7, 20)
        end_date = date(2018, 7, 31)
        valid_incarceration_sentence = StateIncarcerationSentence.new_with_defaults(
            state_code='US_XX',
            incarceration_sentence_id=1,
            external_id='is1',
            start_date=date(2018, 7, 1),
            completion_date=date(2018, 7, 30),
            status=StateSentenceStatus.PRESENT_WITHOUT_INFO)
        valid_incarceration_sentence_2 = StateIncarcerationSentence.new_with_defaults(
            state_code='US_XX',
            incarceration_sentence_id=1,
            external_id='is1',
            start_date=date(2018, 7, 1),
            completion_date=date(2018, 7, 20),
            status=StateSentenceStatus.PRESENT_WITHOUT_INFO)
        invalid_incarceration_sentence_2 = StateIncarcerationSentence.new_with_defaults(
            state_code='US_XX',
            incarceration_sentence_id=1,
            external_id='is1',
            start_date=date(2018, 8, 21),
            completion_date=date(2018, 8, 20),
            status=StateSentenceStatus.PRESENT_WITHOUT_INFO)

        self.assertEqual([valid_incarceration_sentence, valid_incarceration_sentence_2],
                         _get_sentences_overlapping_with_dates(start_date, end_date,
                                                               [valid_incarceration_sentence,
                                                                valid_incarceration_sentence_2,
                                                                invalid_incarceration_sentence_2]))


class TestGetSupervisionPeriodSupervisionTypeFromSentence(unittest.TestCase):
    def test_get_supervision_period_supervision_type_from_sentence(self):
        supervision_sentence = StateSupervisionSentence.new_with_defaults(
            state_code='US_XX',
            supervision_sentence_id=111,
            status=StateSentenceStatus.PRESENT_WITHOUT_INFO
        )

        for supervision_type in StateSupervisionType:
            supervision_sentence.supervision_type = supervision_type

            # Assert this doesn't fail for all possible supervision types
            _ = _get_sentence_supervision_type_from_sentence(supervision_sentence)

    def test_get_supervision_period_supervision_type_from_sentence_incarceration_sentence(self):
        incarceration_sentence = StateIncarcerationSentence.new_with_defaults(
            state_code='US_XX',
            incarceration_sentence_id=111,
            status=StateSentenceStatus.PRESENT_WITHOUT_INFO
        )

        supervision_type = _get_sentence_supervision_type_from_sentence(incarceration_sentence)

        self.assertEqual(StateSupervisionType.PAROLE, supervision_type)
