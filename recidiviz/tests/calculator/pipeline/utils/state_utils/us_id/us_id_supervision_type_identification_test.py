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
"""Tests for us_id_supervision_type_identification.py"""
import unittest
from datetime import date

from dateutil.relativedelta import relativedelta
from freezegun import freeze_time

from recidiviz.calculator.pipeline.utils.state_utils.us_id.us_id_supervision_type_identification import \
    us_id_get_pre_incarceration_supervision_type, \
    us_id_get_most_recent_supervision_period_supervision_type_before_upper_bound_day, \
    INCARCERATION_SUPERVISION_TYPE_DAYS_LIMIT, us_id_get_post_incarceration_supervision_type
from recidiviz.common.constants.state.state_incarceration_period import StateIncarcerationPeriodAdmissionReason, \
    StateIncarcerationPeriodReleaseReason
from recidiviz.common.constants.state.state_supervision_period import StateSupervisionPeriodSupervisionType
from recidiviz.persistence.entity.state.entities import StateIncarcerationPeriod, StateSupervisionPeriod, \
    StateSupervisionSentence


class UsIdGetPreIncarcerationSupervisionTypeTest(unittest.TestCase):
    """Tests for us_id_get_pre_incarceration_supervision_type"""

    def test_usId_getPreIncarcerationSupervisionType(self):
        incarceration_period = StateIncarcerationPeriod.new_with_defaults(
            incarceration_period_id=1,
            admission_reason=StateIncarcerationPeriodAdmissionReason.RETURN_FROM_SUPERVISION,
            external_id='ip1',
            state_code='US_ID',
            admission_date=date(2019, 9, 13))

        preceding_supervision_period = StateSupervisionPeriod.new_with_defaults(
            supervision_period_id=1,
            start_date=
            incarceration_period.admission_date -
            relativedelta(days=INCARCERATION_SUPERVISION_TYPE_DAYS_LIMIT + 100),
            termination_date=
            incarceration_period.admission_date -
            relativedelta(days=INCARCERATION_SUPERVISION_TYPE_DAYS_LIMIT - 1),
            supervision_period_supervision_type=StateSupervisionPeriodSupervisionType.PAROLE
        )

        supervision_sentence = StateSupervisionSentence.new_with_defaults(
            supervision_sentence_id=1,
            external_id='XXX',
            supervision_periods=[preceding_supervision_period])

        self.assertEqual(StateSupervisionPeriodSupervisionType.PAROLE,
                         us_id_get_pre_incarceration_supervision_type(
                             incarceration_sentences=[],
                             supervision_sentences=[supervision_sentence],
                             incarceration_period=incarceration_period))

    def test_usId_getPreIncarcerationSupervisionType_ignoreOutOfDatePeriods_After(self):
        incarceration_period = StateIncarcerationPeriod.new_with_defaults(
            incarceration_period_id=1,
            admission_reason=StateIncarcerationPeriodAdmissionReason.RETURN_FROM_SUPERVISION,
            external_id='ip1',
            state_code='US_ID',
            admission_date=date(2019, 9, 13))

        preceding_supervision_period = StateSupervisionPeriod.new_with_defaults(
            supervision_period_id=1,
            start_date=date(2020, 3, 3),
            termination_date=date(2020, 10, 13),
            supervision_period_supervision_type=StateSupervisionPeriodSupervisionType.PAROLE
        )

        supervision_sentence = StateSupervisionSentence.new_with_defaults(
            supervision_sentence_id=1,
            external_id='XXX',
            supervision_periods=[preceding_supervision_period])

        self.assertIsNone(us_id_get_pre_incarceration_supervision_type(
            incarceration_sentences=[],
            supervision_sentences=[supervision_sentence],
            incarceration_period=incarceration_period))

    def test_usId_getPreIncarcerationSupervisionType_ignoreOutOfDatePeriods_Before(self):
        incarceration_period = StateIncarcerationPeriod.new_with_defaults(
            incarceration_period_id=1,
            admission_reason=StateIncarcerationPeriodAdmissionReason.RETURN_FROM_SUPERVISION,
            external_id='ip1',
            state_code='US_ID',
            admission_date=date(2019, 9, 13))

        preceding_supervision_period = StateSupervisionPeriod.new_with_defaults(
            supervision_period_id=1,
            start_date=
            incarceration_period.admission_date -
            relativedelta(days=INCARCERATION_SUPERVISION_TYPE_DAYS_LIMIT + 100),
            termination_date=
            incarceration_period.admission_date -
            relativedelta(days=INCARCERATION_SUPERVISION_TYPE_DAYS_LIMIT + 10),
            supervision_period_supervision_type=StateSupervisionPeriodSupervisionType.PAROLE
        )

        supervision_sentence = StateSupervisionSentence.new_with_defaults(
            supervision_sentence_id=1,
            external_id='XXX',
            supervision_periods=[preceding_supervision_period])

        self.assertIsNone(us_id_get_pre_incarceration_supervision_type(
            incarceration_sentences=[],
            supervision_sentences=[supervision_sentence],
            incarceration_period=incarceration_period))


class UsIdGetPostIncarcerationSupervisionTypeTest(unittest.TestCase):
    """Tests for us_id_get_post_incarceration_supervision_type"""

    def test_usId_getPostIncarcerationSupervisionType(self):
        incarceration_period = StateIncarcerationPeriod.new_with_defaults(
            incarceration_period_id=1,
            admission_reason=StateIncarcerationPeriodAdmissionReason.RETURN_FROM_SUPERVISION,
            external_id='ip1',
            state_code='US_ID',
            admission_date=date(2019, 9, 13),
            release_date=date(2020, 3, 1),
            release_reason=StateIncarcerationPeriodReleaseReason.CONDITIONAL_RELEASE
        )

        succeeding_supervision_period = StateSupervisionPeriod.new_with_defaults(
            supervision_period_id=1,
            start_date=incarceration_period.release_date,
            supervision_period_supervision_type=StateSupervisionPeriodSupervisionType.PAROLE
        )

        supervision_sentence = StateSupervisionSentence.new_with_defaults(
            supervision_sentence_id=1,
            external_id='XXX',
            supervision_periods=[succeeding_supervision_period])

        self.assertEqual(StateSupervisionPeriodSupervisionType.PAROLE,
                         us_id_get_post_incarceration_supervision_type(
                             incarceration_sentences=[],
                             supervision_sentences=[supervision_sentence],
                             incarceration_period=incarceration_period))

    def test_usId_getPostIncarcerationSupervisionType_ignoreOutOfDatePeriods_After(self):
        incarceration_period = StateIncarcerationPeriod.new_with_defaults(
            incarceration_period_id=1,
            admission_reason=StateIncarcerationPeriodAdmissionReason.RETURN_FROM_SUPERVISION,
            external_id='ip1',
            state_code='US_ID',
            admission_date=date(2019, 9, 13),
            release_date=date(2020, 1, 4),
            release_reason=StateIncarcerationPeriodReleaseReason.CONDITIONAL_RELEASE
        )

        succeeding_supervision_period = StateSupervisionPeriod.new_with_defaults(
            supervision_period_id=1,
            start_date=
            incarceration_period.release_date +
            relativedelta(days=(INCARCERATION_SUPERVISION_TYPE_DAYS_LIMIT + 100)),
            supervision_period_supervision_type=StateSupervisionPeriodSupervisionType.PAROLE
        )

        supervision_sentence = StateSupervisionSentence.new_with_defaults(
            supervision_sentence_id=1,
            external_id='XXX',
            supervision_periods=[succeeding_supervision_period])

        self.assertIsNone(us_id_get_post_incarceration_supervision_type(
            incarceration_sentences=[],
            supervision_sentences=[supervision_sentence],
            incarceration_period=incarceration_period))

    def test_usId_getPostIncarcerationSupervisionType_ignoreOutOfDatePeriods_Before(self):
        incarceration_period = StateIncarcerationPeriod.new_with_defaults(
            incarceration_period_id=1,
            admission_reason=StateIncarcerationPeriodAdmissionReason.RETURN_FROM_SUPERVISION,
            external_id='ip1',
            state_code='US_ID',
            admission_date=date(2019, 9, 13),
            release_date=date(2020, 1, 18),
            release_reason=StateIncarcerationPeriodReleaseReason.CONDITIONAL_RELEASE
        )

        succeeding_supervision_period = StateSupervisionPeriod.new_with_defaults(
            supervision_period_id=1,
            start_date=
            incarceration_period.admission_date -
            relativedelta(days=INCARCERATION_SUPERVISION_TYPE_DAYS_LIMIT + 100),
            termination_date=
            incarceration_period.admission_date -
            relativedelta(days=INCARCERATION_SUPERVISION_TYPE_DAYS_LIMIT + 10),
            supervision_period_supervision_type=StateSupervisionPeriodSupervisionType.PAROLE
        )

        supervision_sentence = StateSupervisionSentence.new_with_defaults(
            supervision_sentence_id=1,
            external_id='XXX',
            supervision_periods=[succeeding_supervision_period])

        self.assertIsNone(us_id_get_post_incarceration_supervision_type(
            incarceration_sentences=[],
            supervision_sentences=[supervision_sentence],
            incarceration_period=incarceration_period))

    def test_usId_getPostIncarcerationSupervisionType_NotConditionalRelease(self):
        incarceration_period = StateIncarcerationPeriod.new_with_defaults(
            incarceration_period_id=1,
            admission_reason=StateIncarcerationPeriodAdmissionReason.RETURN_FROM_SUPERVISION,
            external_id='ip1',
            state_code='US_ID',
            admission_date=date(2019, 9, 13),
            release_date=date(2020, 3, 1),
            release_reason=StateIncarcerationPeriodReleaseReason.SENTENCE_SERVED
        )

        self.assertIsNone(us_id_get_post_incarceration_supervision_type(
            incarceration_sentences=[],
            supervision_sentences=[],
            incarceration_period=incarceration_period))


class UsIdGetMostRecentSupervisionPeriodSupervisionTypeBeforeUpperBoundDayTest(unittest.TestCase):
    """Unittests for the us_id_get_most_recent_supervision_period_supervision_type_before_upper_bound_day helper."""

    def setUp(self) -> None:
        self.upper_bound_date = date(2018, 10, 10)

    def test_most_recent_supervision_type_no_lower_bound(self):
        preceding_supervision_period = StateSupervisionPeriod.new_with_defaults(
            supervision_period_id=1,
            start_date=self.upper_bound_date - relativedelta(days=100),
            termination_date=self.upper_bound_date - relativedelta(days=10),
            supervision_period_supervision_type=StateSupervisionPeriodSupervisionType.PAROLE
        )

        supervision_period_supervision_type = \
            us_id_get_most_recent_supervision_period_supervision_type_before_upper_bound_day(
                upper_bound_exclusive_date=self.upper_bound_date,
                lower_bound_inclusive_date=None,
                supervision_periods=[preceding_supervision_period])

        self.assertEqual(StateSupervisionPeriodSupervisionType.PAROLE, supervision_period_supervision_type)

    def test_most_recent_supervision_type_no_lower_bound_incarceration_sentence(self):
        preceding_supervision_period = StateSupervisionPeriod.new_with_defaults(
            supervision_period_id=1,
            start_date=self.upper_bound_date - relativedelta(days=100),
            termination_date=self.upper_bound_date - relativedelta(days=10),
            supervision_period_supervision_type=StateSupervisionPeriodSupervisionType.PAROLE
        )

        supervision_period_supervision_type = \
            us_id_get_most_recent_supervision_period_supervision_type_before_upper_bound_day(
                upper_bound_exclusive_date=self.upper_bound_date,
                lower_bound_inclusive_date=None,
                supervision_periods=[preceding_supervision_period])

        self.assertEqual(StateSupervisionPeriodSupervisionType.PAROLE, supervision_period_supervision_type)

    def test_most_recent_supervision_type_lower_bound(self):
        preceding_supervision_period = StateSupervisionPeriod.new_with_defaults(
            supervision_period_id=1,
            start_date=self.upper_bound_date - relativedelta(days=100),
            termination_date=self.upper_bound_date - relativedelta(days=10),
            supervision_period_supervision_type=StateSupervisionPeriodSupervisionType.PAROLE
        )

        supervision_period_supervision_type = \
            us_id_get_most_recent_supervision_period_supervision_type_before_upper_bound_day(
                upper_bound_exclusive_date=self.upper_bound_date,
                lower_bound_inclusive_date=None,
                supervision_periods=[preceding_supervision_period])

        self.assertEqual(StateSupervisionPeriodSupervisionType.PAROLE, supervision_period_supervision_type)

    def test_most_recent_supervision_type_ignore_before_lower_bound(self):
        preceding_supervision_period = StateSupervisionPeriod.new_with_defaults(
            supervision_period_id=1,
            start_date=self.upper_bound_date - relativedelta(days=100),
            termination_date=self.upper_bound_date - relativedelta(days=10),
            supervision_period_supervision_type=StateSupervisionPeriodSupervisionType.PAROLE
        )

        supervision_period_supervision_type = \
            us_id_get_most_recent_supervision_period_supervision_type_before_upper_bound_day(
                upper_bound_exclusive_date=self.upper_bound_date,
                lower_bound_inclusive_date=self.upper_bound_date - relativedelta(days=2),
                supervision_periods=[preceding_supervision_period])

        self.assertEqual(None, supervision_period_supervision_type)

    @freeze_time('2000-01-31')
    def test_most_recent_supervision_type_active_supervision(self):
        preceding_supervision_period = StateSupervisionPeriod.new_with_defaults(
            supervision_period_id=1,
            start_date=date.today() - relativedelta(days=100),
            termination_date=None,
            supervision_period_supervision_type=StateSupervisionPeriodSupervisionType.PROBATION
        )

        supervision_period_supervision_type = \
            us_id_get_most_recent_supervision_period_supervision_type_before_upper_bound_day(
                upper_bound_exclusive_date=self.upper_bound_date,
                lower_bound_inclusive_date=None,
                supervision_periods=[preceding_supervision_period])

        self.assertEqual(StateSupervisionPeriodSupervisionType.PROBATION, supervision_period_supervision_type)

    def test_most_recent_supervision_type_overlapping_supervision(self):
        preceding_supervision_period = StateSupervisionPeriod.new_with_defaults(
            supervision_period_id=1,
            start_date=self.upper_bound_date - relativedelta(days=100),
            termination_date=self.upper_bound_date + relativedelta(days=100),
            supervision_period_supervision_type=StateSupervisionPeriodSupervisionType.PROBATION
        )

        supervision_period_supervision_type = \
            us_id_get_most_recent_supervision_period_supervision_type_before_upper_bound_day(
                upper_bound_exclusive_date=self.upper_bound_date,
                lower_bound_inclusive_date=None,
                supervision_periods=[preceding_supervision_period])

        self.assertEqual(StateSupervisionPeriodSupervisionType.PROBATION, supervision_period_supervision_type)

    def test_most_recent_supervision_type_supervision_terminates_on_bound(self):
        preceding_supervision_period = StateSupervisionPeriod.new_with_defaults(
            supervision_period_id=1,
            start_date=self.upper_bound_date - relativedelta(days=100),
            termination_date=self.upper_bound_date,
            supervision_period_supervision_type=StateSupervisionPeriodSupervisionType.PAROLE
        )

        supervision_period_supervision_type = \
            us_id_get_most_recent_supervision_period_supervision_type_before_upper_bound_day(
                upper_bound_exclusive_date=self.upper_bound_date,
                lower_bound_inclusive_date=None,
                supervision_periods=[preceding_supervision_period])

        self.assertEqual(StateSupervisionPeriodSupervisionType.PAROLE, supervision_period_supervision_type)

    def test_most_recent_supervision_type_supervision_starts_on_bound(self):
        preceding_supervision_period = StateSupervisionPeriod.new_with_defaults(
            supervision_period_id=1,
            start_date=self.upper_bound_date,
            termination_date=self.upper_bound_date + relativedelta(years=1),
            supervision_period_supervision_type=StateSupervisionPeriodSupervisionType.PAROLE
        )

        supervision_period_supervision_type = \
            us_id_get_most_recent_supervision_period_supervision_type_before_upper_bound_day(
                upper_bound_exclusive_date=self.upper_bound_date,
                lower_bound_inclusive_date=None,
                supervision_periods=[preceding_supervision_period])

        self.assertEqual(supervision_period_supervision_type, None)

    def test_most_recent_supervision_type_no_sentences_no_lower_bound(self):
        supervision_period_supervision_type = \
            us_id_get_most_recent_supervision_period_supervision_type_before_upper_bound_day(
                upper_bound_exclusive_date=self.upper_bound_date,
                lower_bound_inclusive_date=None,
                supervision_periods=[])

        self.assertEqual(supervision_period_supervision_type, None)

    def test_most_recent_supervision_type_no_sentences_same_day_bound(self):
        supervision_period_supervision_type = \
            us_id_get_most_recent_supervision_period_supervision_type_before_upper_bound_day(
                upper_bound_exclusive_date=self.upper_bound_date,
                lower_bound_inclusive_date=self.upper_bound_date,
                supervision_periods=[])

        self.assertEqual(supervision_period_supervision_type, None)
