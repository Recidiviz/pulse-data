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
from typing import Optional

from dateutil.relativedelta import relativedelta
from freezegun import freeze_time

from recidiviz.calculator.pipeline.supervision.supervision_time_bucket import RevocationReturnSupervisionTimeBucket
from recidiviz.calculator.pipeline.utils.state_utils.us_id.us_id_supervision_type_identification import \
    us_id_get_pre_incarceration_supervision_type, \
    us_id_get_most_recent_supervision_period_supervision_type_before_upper_bound_day, \
    SUPERVISION_TYPE_LOOKBACK_DAYS_LIMIT, us_id_get_post_incarceration_supervision_type, \
    us_id_supervision_period_is_out_of_state, us_id_get_supervision_period_admission_override
from recidiviz.calculator.pipeline.utils.supervision_period_index import SupervisionPeriodIndex
from recidiviz.common.constants.state.state_incarceration_period import StateIncarcerationPeriodAdmissionReason, \
    StateIncarcerationPeriodReleaseReason, StateIncarcerationPeriodStatus
from recidiviz.common.constants.state.state_sentence import StateSentenceStatus
from recidiviz.common.constants.state.state_supervision_period import StateSupervisionPeriodSupervisionType, \
    StateSupervisionPeriodAdmissionReason, StateSupervisionPeriodTerminationReason, StateSupervisionPeriodStatus
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
            admission_date=date(2019, 9, 13),
            status=StateIncarcerationPeriodStatus.PRESENT_WITHOUT_INFO)

        preceding_supervision_period = StateSupervisionPeriod.new_with_defaults(
            state_code='US_ID',
            supervision_period_id=1,
            start_date=incarceration_period.admission_date -
            relativedelta(days=SUPERVISION_TYPE_LOOKBACK_DAYS_LIMIT + 100),
            termination_date=incarceration_period.admission_date -
            relativedelta(days=SUPERVISION_TYPE_LOOKBACK_DAYS_LIMIT - 1),
            supervision_period_supervision_type=StateSupervisionPeriodSupervisionType.PAROLE,
            status=StateSupervisionPeriodStatus.PRESENT_WITHOUT_INFO
        )

        supervision_sentence = StateSupervisionSentence.new_with_defaults(
            state_code='US_ID',
            supervision_sentence_id=1,
            external_id='XXX',
            supervision_periods=[preceding_supervision_period],
            status=StateSentenceStatus.PRESENT_WITHOUT_INFO)

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
            admission_date=date(2019, 9, 13),
            status=StateIncarcerationPeriodStatus.PRESENT_WITHOUT_INFO)

        preceding_supervision_period = StateSupervisionPeriod.new_with_defaults(
            state_code='US_ID',
            supervision_period_id=1,
            start_date=date(2020, 3, 3),
            termination_date=date(2020, 10, 13),
            supervision_period_supervision_type=StateSupervisionPeriodSupervisionType.PAROLE,
            status=StateSupervisionPeriodStatus.PRESENT_WITHOUT_INFO
        )

        supervision_sentence = StateSupervisionSentence.new_with_defaults(
            state_code='US_ID',
            supervision_sentence_id=1,
            external_id='XXX',
            supervision_periods=[preceding_supervision_period],
            status=StateSentenceStatus.PRESENT_WITHOUT_INFO)

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
            admission_date=date(2019, 9, 13),
            status=StateIncarcerationPeriodStatus.PRESENT_WITHOUT_INFO)

        preceding_supervision_period = StateSupervisionPeriod.new_with_defaults(
            state_code='US_ID',
            supervision_period_id=1,
            start_date=incarceration_period.admission_date -
            relativedelta(days=SUPERVISION_TYPE_LOOKBACK_DAYS_LIMIT + 100),
            termination_date=incarceration_period.admission_date -
            relativedelta(days=SUPERVISION_TYPE_LOOKBACK_DAYS_LIMIT + 10),
            supervision_period_supervision_type=StateSupervisionPeriodSupervisionType.PAROLE,
            status=StateSupervisionPeriodStatus.PRESENT_WITHOUT_INFO
        )

        supervision_sentence = StateSupervisionSentence.new_with_defaults(
            state_code='US_ID',
            supervision_sentence_id=1,
            external_id='XXX',
            supervision_periods=[preceding_supervision_period],
            status=StateSentenceStatus.PRESENT_WITHOUT_INFO)

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
            release_reason=StateIncarcerationPeriodReleaseReason.CONDITIONAL_RELEASE,
            status=StateIncarcerationPeriodStatus.PRESENT_WITHOUT_INFO
        )

        succeeding_supervision_period = StateSupervisionPeriod.new_with_defaults(
            state_code='US_ID',
            supervision_period_id=1,
            start_date=incarceration_period.release_date,
            supervision_period_supervision_type=StateSupervisionPeriodSupervisionType.PAROLE,
            status=StateSupervisionPeriodStatus.PRESENT_WITHOUT_INFO
        )

        supervision_sentence = StateSupervisionSentence.new_with_defaults(
            state_code='US_ID',
            supervision_sentence_id=1,
            external_id='XXX',
            supervision_periods=[succeeding_supervision_period],
            status=StateSentenceStatus.PRESENT_WITHOUT_INFO)

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
            release_reason=StateIncarcerationPeriodReleaseReason.CONDITIONAL_RELEASE,
            status=StateIncarcerationPeriodStatus.PRESENT_WITHOUT_INFO
        )

        succeeding_supervision_period = StateSupervisionPeriod.new_with_defaults(
            state_code='US_ID',
            supervision_period_id=1,
            start_date=incarceration_period.release_date +
            relativedelta(days=(SUPERVISION_TYPE_LOOKBACK_DAYS_LIMIT + 100)),
            supervision_period_supervision_type=StateSupervisionPeriodSupervisionType.PAROLE,
            status=StateSupervisionPeriodStatus.PRESENT_WITHOUT_INFO
        )

        supervision_sentence = StateSupervisionSentence.new_with_defaults(
            state_code='US_ID',
            supervision_sentence_id=1,
            external_id='XXX',
            supervision_periods=[succeeding_supervision_period],
            status=StateSentenceStatus.PRESENT_WITHOUT_INFO)

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
            release_reason=StateIncarcerationPeriodReleaseReason.CONDITIONAL_RELEASE,
            status=StateIncarcerationPeriodStatus.PRESENT_WITHOUT_INFO
        )

        succeeding_supervision_period = StateSupervisionPeriod.new_with_defaults(
            state_code='US_ID',
            supervision_period_id=1,
            start_date=incarceration_period.admission_date -
            relativedelta(days=SUPERVISION_TYPE_LOOKBACK_DAYS_LIMIT + 100),
            termination_date=incarceration_period.admission_date -
            relativedelta(days=SUPERVISION_TYPE_LOOKBACK_DAYS_LIMIT + 10),
            supervision_period_supervision_type=StateSupervisionPeriodSupervisionType.PAROLE,
            status=StateSupervisionPeriodStatus.PRESENT_WITHOUT_INFO
        )

        supervision_sentence = StateSupervisionSentence.new_with_defaults(
            state_code='US_ID',
            supervision_sentence_id=1,
            external_id='XXX',
            supervision_periods=[succeeding_supervision_period],
            status=StateSentenceStatus.PRESENT_WITHOUT_INFO)

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
            release_reason=StateIncarcerationPeriodReleaseReason.SENTENCE_SERVED,
            status=StateIncarcerationPeriodStatus.PRESENT_WITHOUT_INFO
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
            state_code='US_ID',
            supervision_period_id=1,
            start_date=self.upper_bound_date - relativedelta(days=100),
            termination_date=self.upper_bound_date - relativedelta(days=10),
            supervision_period_supervision_type=StateSupervisionPeriodSupervisionType.PAROLE,
            status=StateSupervisionPeriodStatus.PRESENT_WITHOUT_INFO
        )

        supervision_period_supervision_type = \
            us_id_get_most_recent_supervision_period_supervision_type_before_upper_bound_day(
                upper_bound_exclusive_date=self.upper_bound_date,
                lower_bound_inclusive_date=None,
                supervision_periods=[preceding_supervision_period])

        self.assertEqual(StateSupervisionPeriodSupervisionType.PAROLE, supervision_period_supervision_type)

    def test_most_recent_supervision_type_no_lower_bound_incarceration_sentence(self):
        preceding_supervision_period = StateSupervisionPeriod.new_with_defaults(
            state_code='US_ID',
            supervision_period_id=1,
            start_date=self.upper_bound_date - relativedelta(days=100),
            termination_date=self.upper_bound_date - relativedelta(days=10),
            supervision_period_supervision_type=StateSupervisionPeriodSupervisionType.PAROLE,
            status=StateSupervisionPeriodStatus.PRESENT_WITHOUT_INFO
        )

        supervision_period_supervision_type = \
            us_id_get_most_recent_supervision_period_supervision_type_before_upper_bound_day(
                upper_bound_exclusive_date=self.upper_bound_date,
                lower_bound_inclusive_date=None,
                supervision_periods=[preceding_supervision_period])

        self.assertEqual(StateSupervisionPeriodSupervisionType.PAROLE, supervision_period_supervision_type)

    def test_most_recent_supervision_type_lower_bound(self):
        preceding_supervision_period = StateSupervisionPeriod.new_with_defaults(
            state_code='US_ID',
            supervision_period_id=1,
            start_date=self.upper_bound_date - relativedelta(days=100),
            termination_date=self.upper_bound_date - relativedelta(days=10),
            supervision_period_supervision_type=StateSupervisionPeriodSupervisionType.PAROLE,
            status=StateSupervisionPeriodStatus.PRESENT_WITHOUT_INFO
        )

        supervision_period_supervision_type = \
            us_id_get_most_recent_supervision_period_supervision_type_before_upper_bound_day(
                upper_bound_exclusive_date=self.upper_bound_date,
                lower_bound_inclusive_date=None,
                supervision_periods=[preceding_supervision_period])

        self.assertEqual(StateSupervisionPeriodSupervisionType.PAROLE, supervision_period_supervision_type)

    def test_most_recent_supervision_type_ignore_before_lower_bound(self):
        preceding_supervision_period = StateSupervisionPeriod.new_with_defaults(
            state_code='US_ID',
            supervision_period_id=1,
            start_date=self.upper_bound_date - relativedelta(days=100),
            termination_date=self.upper_bound_date - relativedelta(days=10),
            supervision_period_supervision_type=StateSupervisionPeriodSupervisionType.PAROLE,
            status=StateSupervisionPeriodStatus.PRESENT_WITHOUT_INFO
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
            state_code='US_ID',
            supervision_period_id=1,
            start_date=date.today() - relativedelta(days=100),
            termination_date=None,
            supervision_period_supervision_type=StateSupervisionPeriodSupervisionType.PROBATION,
            status=StateSupervisionPeriodStatus.PRESENT_WITHOUT_INFO
        )

        supervision_period_supervision_type = \
            us_id_get_most_recent_supervision_period_supervision_type_before_upper_bound_day(
                upper_bound_exclusive_date=self.upper_bound_date,
                lower_bound_inclusive_date=None,
                supervision_periods=[preceding_supervision_period])

        self.assertEqual(StateSupervisionPeriodSupervisionType.PROBATION, supervision_period_supervision_type)

    def test_most_recent_supervision_type_overlapping_supervision(self):
        preceding_supervision_period = StateSupervisionPeriod.new_with_defaults(
            state_code='US_ID',
            supervision_period_id=1,
            start_date=self.upper_bound_date - relativedelta(days=100),
            termination_date=self.upper_bound_date + relativedelta(days=100),
            supervision_period_supervision_type=StateSupervisionPeriodSupervisionType.PROBATION,
            status=StateSupervisionPeriodStatus.PRESENT_WITHOUT_INFO
        )

        supervision_period_supervision_type = \
            us_id_get_most_recent_supervision_period_supervision_type_before_upper_bound_day(
                upper_bound_exclusive_date=self.upper_bound_date,
                lower_bound_inclusive_date=None,
                supervision_periods=[preceding_supervision_period])

        self.assertEqual(StateSupervisionPeriodSupervisionType.PROBATION, supervision_period_supervision_type)

    def test_most_recent_supervision_type_supervision_terminates_on_bound(self):
        preceding_supervision_period = StateSupervisionPeriod.new_with_defaults(
            state_code='US_ID',
            supervision_period_id=1,
            start_date=self.upper_bound_date - relativedelta(days=100),
            termination_date=self.upper_bound_date,
            supervision_period_supervision_type=StateSupervisionPeriodSupervisionType.PAROLE,
            status=StateSupervisionPeriodStatus.PRESENT_WITHOUT_INFO
        )

        supervision_period_supervision_type = \
            us_id_get_most_recent_supervision_period_supervision_type_before_upper_bound_day(
                upper_bound_exclusive_date=self.upper_bound_date,
                lower_bound_inclusive_date=None,
                supervision_periods=[preceding_supervision_period])

        self.assertEqual(StateSupervisionPeriodSupervisionType.PAROLE, supervision_period_supervision_type)

    def test_most_recent_supervision_type_supervision_starts_on_bound(self):
        preceding_supervision_period = StateSupervisionPeriod.new_with_defaults(
            state_code='US_ID',
            supervision_period_id=1,
            start_date=self.upper_bound_date,
            termination_date=self.upper_bound_date + relativedelta(years=1),
            supervision_period_supervision_type=StateSupervisionPeriodSupervisionType.PAROLE,
            status=StateSupervisionPeriodStatus.PRESENT_WITHOUT_INFO
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


class UsIdGetSupervisionPeriodAdmissionOverrideTest(unittest.TestCase):
    """Unittests for the us_id_get_supervision_period_admission_override."""

    def setUp(self) -> None:
        self.upper_bound_date = date(2018, 10, 10)

    def test_usId_getSupervisionPeriodAdmissionOverride_precededByInvestigation_overrideReason(self):
        supervision_period_previous = StateSupervisionPeriod.new_with_defaults(
            state_code='US_ID',
            supervision_period_id=1,
            start_date=self.upper_bound_date - relativedelta(days=100),
            termination_date=self.upper_bound_date - relativedelta(days=10),
            termination_reason=StateSupervisionPeriodTerminationReason.TRANSFER_WITHIN_STATE,
            supervision_period_supervision_type=StateSupervisionPeriodSupervisionType.INVESTIGATION,
            status=StateSupervisionPeriodStatus.PRESENT_WITHOUT_INFO)
        supervision_period = StateSupervisionPeriod.new_with_defaults(
            state_code='US_ID',
            supervision_period_id=2,
            admission_reason=StateSupervisionPeriodAdmissionReason.TRANSFER_WITHIN_STATE,
            start_date=supervision_period_previous.termination_date,
            supervision_period_supervision_type=StateSupervisionPeriodSupervisionType.PAROLE,
            status=StateSupervisionPeriodStatus.PRESENT_WITHOUT_INFO)
        idx = SupervisionPeriodIndex(supervision_periods=[supervision_period, supervision_period_previous])
        found_admission_reason = us_id_get_supervision_period_admission_override(
            supervision_period=supervision_period, supervision_period_index=idx)
        self.assertEqual(StateSupervisionPeriodAdmissionReason.COURT_SENTENCE, found_admission_reason)

    def testUsIdGetSupervisionPeriodAdmissionOverride_investigationTooFarBack(self):
        supervision_period_previous = StateSupervisionPeriod.new_with_defaults(
            state_code='US_ID',
            supervision_period_id=1,
            start_date=self.upper_bound_date - relativedelta(days=100),
            termination_date=self.upper_bound_date - relativedelta(days=10),
            termination_reason=StateSupervisionPeriodTerminationReason.TRANSFER_WITHIN_STATE,
            supervision_period_supervision_type=StateSupervisionPeriodSupervisionType.INVESTIGATION,
            status=StateSupervisionPeriodStatus.PRESENT_WITHOUT_INFO)
        supervision_period = StateSupervisionPeriod.new_with_defaults(
            state_code='US_ID',
            supervision_period_id=2,
            admission_reason=StateSupervisionPeriodAdmissionReason.TRANSFER_WITHIN_STATE,
            start_date=supervision_period_previous.termination_date + relativedelta(
                days=SUPERVISION_TYPE_LOOKBACK_DAYS_LIMIT + 1),
            supervision_period_supervision_type=StateSupervisionPeriodSupervisionType.PAROLE,
            status=StateSupervisionPeriodStatus.PRESENT_WITHOUT_INFO)
        idx = SupervisionPeriodIndex(supervision_periods=[supervision_period, supervision_period_previous])
        found_admission_reason = us_id_get_supervision_period_admission_override(
            supervision_period=supervision_period, supervision_period_index=idx)
        self.assertEqual(supervision_period.admission_reason, found_admission_reason)

    def testUsIdGetSupervisionPeriodAdmissionOverride_notPrecededByInvestigation(self):
        supervision_period_previous = StateSupervisionPeriod.new_with_defaults(
            state_code='US_ID',
            supervision_period_id=1,
            start_date=self.upper_bound_date - relativedelta(days=100),
            termination_date=self.upper_bound_date - relativedelta(days=10),
            termination_reason=StateSupervisionPeriodTerminationReason.TRANSFER_WITHIN_STATE,
            supervision_period_supervision_type=StateSupervisionPeriodSupervisionType.PAROLE,
            status=StateSupervisionPeriodStatus.PRESENT_WITHOUT_INFO)
        supervision_period = StateSupervisionPeriod.new_with_defaults(
            state_code='US_ID',
            supervision_period_id=2,
            admission_reason=StateSupervisionPeriodAdmissionReason.TRANSFER_WITHIN_STATE,
            start_date=supervision_period_previous.termination_date,
            supervision_period_supervision_type=StateSupervisionPeriodSupervisionType.PAROLE,
            status=StateSupervisionPeriodStatus.PRESENT_WITHOUT_INFO)
        idx = SupervisionPeriodIndex(supervision_periods=[supervision_period, supervision_period_previous])
        found_admission_reason = us_id_get_supervision_period_admission_override(
            supervision_period=supervision_period, supervision_period_index=idx)
        self.assertEqual(supervision_period.admission_reason, found_admission_reason)

    def testUsIdGetSupervisionPeriodAdmissionReasonOverride_ignoreNullSupervisionType(self):
        supervision_period_previous = StateSupervisionPeriod.new_with_defaults(
            state_code='US_ID',
            supervision_period_id=1,
            start_date=self.upper_bound_date - relativedelta(days=100),
            termination_date=self.upper_bound_date - relativedelta(days=10),
            termination_reason=StateSupervisionPeriodTerminationReason.TRANSFER_WITHIN_STATE,
            supervision_period_supervision_type=StateSupervisionPeriodSupervisionType.INVESTIGATION,
            status=StateSupervisionPeriodStatus.PRESENT_WITHOUT_INFO)
        supervision_period_one_day = StateSupervisionPeriod.new_with_defaults(
            state_code='US_ID',
            supervision_period_id=2,
            admission_reason=StateSupervisionPeriodAdmissionReason.TRANSFER_WITHIN_STATE,
            start_date=supervision_period_previous.termination_date,
            termination_date=supervision_period_previous.termination_date + relativedelta(days=10),
            status=StateSupervisionPeriodStatus.PRESENT_WITHOUT_INFO)
        supervision_period_ongoing = StateSupervisionPeriod.new_with_defaults(
            state_code='US_ID',
            supervision_period_id=3,
            admission_reason=StateSupervisionPeriodAdmissionReason.TRANSFER_WITHIN_STATE,
            start_date=supervision_period_one_day.termination_date,
            supervision_period_supervision_type=StateSupervisionPeriodSupervisionType.PAROLE,
            status=StateSupervisionPeriodStatus.PRESENT_WITHOUT_INFO)

        idx = SupervisionPeriodIndex(supervision_periods=[
            supervision_period_previous, supervision_period_ongoing, supervision_period_one_day])
        found_admission_reason_for_ongoing = us_id_get_supervision_period_admission_override(
            supervision_period=supervision_period_ongoing, supervision_period_index=idx)
        self.assertEqual(StateSupervisionPeriodAdmissionReason.COURT_SENTENCE, found_admission_reason_for_ongoing)

    def testUsIdGetSupervisionPeriodAdmissionReasonOverride_multiplePeriodsStartOnInvestigationEnd(self):
        supervision_period_previous = StateSupervisionPeriod.new_with_defaults(
            state_code='US_ID',
            supervision_period_id=1,
            start_date=self.upper_bound_date - relativedelta(days=100),
            termination_date=self.upper_bound_date - relativedelta(days=10),
            termination_reason=StateSupervisionPeriodTerminationReason.TRANSFER_WITHIN_STATE,
            supervision_period_supervision_type=StateSupervisionPeriodSupervisionType.INVESTIGATION,
            status=StateSupervisionPeriodStatus.PRESENT_WITHOUT_INFO)
        supervision_period_one_day = StateSupervisionPeriod.new_with_defaults(
            state_code='US_ID',
            supervision_period_id=2,
            admission_reason=StateSupervisionPeriodAdmissionReason.TRANSFER_WITHIN_STATE,
            start_date=supervision_period_previous.termination_date,
            termination_date=supervision_period_previous.termination_date,
            supervision_period_supervision_type=StateSupervisionPeriodSupervisionType.INFORMAL_PROBATION,
            status=StateSupervisionPeriodStatus.PRESENT_WITHOUT_INFO)
        supervision_period_ongoing = StateSupervisionPeriod.new_with_defaults(
            state_code='US_ID',
            supervision_period_id=3,
            admission_reason=StateSupervisionPeriodAdmissionReason.TRANSFER_WITHIN_STATE,
            start_date=supervision_period_one_day.termination_date,
            supervision_period_supervision_type=StateSupervisionPeriodSupervisionType.PAROLE,
            status=StateSupervisionPeriodStatus.PRESENT_WITHOUT_INFO)

        idx = SupervisionPeriodIndex(supervision_periods=[
            supervision_period_previous, supervision_period_ongoing, supervision_period_one_day])
        found_admission_reason_for_one_day = us_id_get_supervision_period_admission_override(
            supervision_period=supervision_period_one_day, supervision_period_index=idx)
        self.assertEqual(StateSupervisionPeriodAdmissionReason.COURT_SENTENCE, found_admission_reason_for_one_day)
        found_admission_reason_for_ongoing = us_id_get_supervision_period_admission_override(
            supervision_period=supervision_period_ongoing, supervision_period_index=idx)
        self.assertEqual(supervision_period_ongoing.admission_reason, found_admission_reason_for_ongoing)


class TestSupervisionPeriodIsOutOfState(unittest.TestCase):
    """Tests the state-specific supervision_period_is_out_of_state function."""

    def test_supervision_period_is_out_of_state_with_identifier_interstate(self):
        self.assertTrue(us_id_supervision_period_is_out_of_state(self.create_time_bucket(
            "INTERSTATE PROBATION - remainder of identifier")))

    def test_supervision_period_is_out_of_state_with_identifier_parole(self):
        self.assertTrue(us_id_supervision_period_is_out_of_state(self.create_time_bucket(
            "PAROLE COMMISSION OFFICE - remainder of identifier")))

    def test_supervision_period_is_out_of_state_with_partial_identifier(self):
        self.assertFalse(us_id_supervision_period_is_out_of_state(self.create_time_bucket(
            "INTERSTATE - remainder of identifier")))

    def test_supervision_period_is_out_of_state_with_incorrect_identifier(self):
        self.assertFalse(us_id_supervision_period_is_out_of_state(self.create_time_bucket("Invalid")))

    def test_supervision_period_is_out_of_state_with_empty_identifier(self):
        self.assertFalse(us_id_supervision_period_is_out_of_state(self.create_time_bucket(None)))

    @staticmethod
    def create_time_bucket(supervising_district_external_id: Optional[str]):
        return RevocationReturnSupervisionTimeBucket(
            state_code="US_ID",
            year=2010,
            month=1,
            event_date=date(2010, 1, 1),
            is_on_supervision_last_day_of_month=False,
            supervision_type=StateSupervisionPeriodSupervisionType.PROBATION,
            supervising_district_external_id=supervising_district_external_id,
            projected_end_date=None,
        )
