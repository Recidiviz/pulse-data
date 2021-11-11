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
"""Tests for us_id_supervision_utils.py"""
import unittest
from datetime import date

from dateutil.relativedelta import relativedelta
from freezegun import freeze_time

from recidiviz.calculator.pipeline.utils.state_utils.us_id.us_id_supervision_period_pre_processing_delegate import (
    SUPERVISION_TYPE_LOOKBACK_MONTH_LIMIT,
)
from recidiviz.calculator.pipeline.utils.state_utils.us_id.us_id_supervision_utils import (
    us_id_get_most_recent_supervision_type_before_upper_bound_day,
    us_id_get_post_incarceration_supervision_type,
)
from recidiviz.common.constants.state.state_incarceration_period import (
    StateIncarcerationPeriodAdmissionReason,
    StateIncarcerationPeriodReleaseReason,
    StateIncarcerationPeriodStatus,
)
from recidiviz.common.constants.state.state_sentence import StateSentenceStatus
from recidiviz.common.constants.state.state_supervision_period import (
    StateSupervisionPeriodSupervisionType,
)
from recidiviz.persistence.entity.state.entities import (
    StateIncarcerationPeriod,
    StateSupervisionPeriod,
    StateSupervisionSentence,
)


class UsIdGetPostIncarcerationSupervisionTypeTest(unittest.TestCase):
    """Tests for us_id_get_post_incarceration_supervision_type"""

    def test_usId_getPostIncarcerationSupervisionType(self) -> None:
        incarceration_period = StateIncarcerationPeriod.new_with_defaults(
            incarceration_period_id=1,
            admission_reason=StateIncarcerationPeriodAdmissionReason.ADMITTED_FROM_SUPERVISION,
            external_id="ip1",
            state_code="US_ID",
            admission_date=date(2019, 9, 13),
            release_date=date(2020, 3, 1),
            release_reason=StateIncarcerationPeriodReleaseReason.CONDITIONAL_RELEASE,
            status=StateIncarcerationPeriodStatus.PRESENT_WITHOUT_INFO,
        )

        succeeding_supervision_period = StateSupervisionPeriod.new_with_defaults(
            state_code="US_ID",
            supervision_period_id=1,
            start_date=incarceration_period.release_date,
            supervision_type=StateSupervisionPeriodSupervisionType.PAROLE,
        )

        supervision_sentence = StateSupervisionSentence.new_with_defaults(
            state_code="US_ID",
            supervision_sentence_id=1,
            external_id="XXX",
            supervision_periods=[succeeding_supervision_period],
            status=StateSentenceStatus.PRESENT_WITHOUT_INFO,
        )

        self.assertEqual(
            StateSupervisionPeriodSupervisionType.PAROLE,
            us_id_get_post_incarceration_supervision_type(
                incarceration_sentences=[],
                supervision_sentences=[supervision_sentence],
                incarceration_period=incarceration_period,
            ),
        )

    def test_usId_getPostIncarcerationSupervisionType_ignoreOutOfDatePeriods_After(
        self,
    ) -> None:
        release_date = date(2020, 1, 4)
        incarceration_period = StateIncarcerationPeriod.new_with_defaults(
            incarceration_period_id=1,
            admission_reason=StateIncarcerationPeriodAdmissionReason.ADMITTED_FROM_SUPERVISION,
            external_id="ip1",
            state_code="US_ID",
            admission_date=date(2019, 9, 13),
            release_date=release_date,
            release_reason=StateIncarcerationPeriodReleaseReason.CONDITIONAL_RELEASE,
            status=StateIncarcerationPeriodStatus.PRESENT_WITHOUT_INFO,
        )

        succeeding_supervision_period = StateSupervisionPeriod.new_with_defaults(
            state_code="US_ID",
            supervision_period_id=1,
            start_date=release_date
            + relativedelta(months=SUPERVISION_TYPE_LOOKBACK_MONTH_LIMIT, days=100),
            supervision_type=StateSupervisionPeriodSupervisionType.PAROLE,
        )

        supervision_sentence = StateSupervisionSentence.new_with_defaults(
            state_code="US_ID",
            supervision_sentence_id=1,
            external_id="XXX",
            supervision_periods=[succeeding_supervision_period],
            status=StateSentenceStatus.PRESENT_WITHOUT_INFO,
        )

        self.assertIsNone(
            us_id_get_post_incarceration_supervision_type(
                incarceration_sentences=[],
                supervision_sentences=[supervision_sentence],
                incarceration_period=incarceration_period,
            )
        )

    def test_usId_getPostIncarcerationSupervisionType_ignoreOutOfDatePeriods_Before(
        self,
    ) -> None:
        admission_date = date(2019, 9, 13)
        incarceration_period = StateIncarcerationPeriod.new_with_defaults(
            incarceration_period_id=1,
            admission_reason=StateIncarcerationPeriodAdmissionReason.ADMITTED_FROM_SUPERVISION,
            external_id="ip1",
            state_code="US_ID",
            admission_date=admission_date,
            release_date=date(2020, 1, 18),
            release_reason=StateIncarcerationPeriodReleaseReason.CONDITIONAL_RELEASE,
            status=StateIncarcerationPeriodStatus.PRESENT_WITHOUT_INFO,
        )

        succeeding_supervision_period = StateSupervisionPeriod.new_with_defaults(
            state_code="US_ID",
            supervision_period_id=1,
            start_date=admission_date
            - relativedelta(months=SUPERVISION_TYPE_LOOKBACK_MONTH_LIMIT, days=100),
            termination_date=admission_date
            - relativedelta(months=SUPERVISION_TYPE_LOOKBACK_MONTH_LIMIT, days=10),
            supervision_type=StateSupervisionPeriodSupervisionType.PAROLE,
        )

        supervision_sentence = StateSupervisionSentence.new_with_defaults(
            state_code="US_ID",
            supervision_sentence_id=1,
            external_id="XXX",
            supervision_periods=[succeeding_supervision_period],
            status=StateSentenceStatus.PRESENT_WITHOUT_INFO,
        )

        self.assertIsNone(
            us_id_get_post_incarceration_supervision_type(
                incarceration_sentences=[],
                supervision_sentences=[supervision_sentence],
                incarceration_period=incarceration_period,
            )
        )

    def test_usId_getPostIncarcerationSupervisionType_NotConditionalRelease(
        self,
    ) -> None:
        incarceration_period = StateIncarcerationPeriod.new_with_defaults(
            incarceration_period_id=1,
            admission_reason=StateIncarcerationPeriodAdmissionReason.ADMITTED_FROM_SUPERVISION,
            external_id="ip1",
            state_code="US_ID",
            admission_date=date(2019, 9, 13),
            release_date=date(2020, 3, 1),
            release_reason=StateIncarcerationPeriodReleaseReason.SENTENCE_SERVED,
            status=StateIncarcerationPeriodStatus.PRESENT_WITHOUT_INFO,
        )

        self.assertIsNone(
            us_id_get_post_incarceration_supervision_type(
                incarceration_sentences=[],
                supervision_sentences=[],
                incarceration_period=incarceration_period,
            )
        )


class UsIdGetMostRecentSupervisionPeriodSupervisionTypeBeforeUpperBoundDayTest(
    unittest.TestCase
):
    """Unittests for the us_id_get_most_recent_supervision_type_before_upper_bound_day helper."""

    def setUp(self) -> None:
        self.upper_bound_date = date(2018, 10, 10)

    def test_most_recent_supervision_type_no_lower_bound(self) -> None:
        preceding_supervision_period = StateSupervisionPeriod.new_with_defaults(
            state_code="US_ID",
            supervision_period_id=1,
            start_date=self.upper_bound_date - relativedelta(days=100),
            termination_date=self.upper_bound_date - relativedelta(days=10),
            supervision_type=StateSupervisionPeriodSupervisionType.PAROLE,
        )

        supervision_type = (
            us_id_get_most_recent_supervision_type_before_upper_bound_day(
                upper_bound_exclusive_date=self.upper_bound_date,
                lower_bound_inclusive_date=None,
                supervision_periods=[preceding_supervision_period],
            )
        )

        self.assertEqual(
            StateSupervisionPeriodSupervisionType.PAROLE,
            supervision_type,
        )

    def test_most_recent_supervision_type_no_lower_bound_incarceration_sentence(
        self,
    ) -> None:
        preceding_supervision_period = StateSupervisionPeriod.new_with_defaults(
            state_code="US_ID",
            supervision_period_id=1,
            start_date=self.upper_bound_date - relativedelta(days=100),
            termination_date=self.upper_bound_date - relativedelta(days=10),
            supervision_type=StateSupervisionPeriodSupervisionType.PAROLE,
        )

        supervision_type = (
            us_id_get_most_recent_supervision_type_before_upper_bound_day(
                upper_bound_exclusive_date=self.upper_bound_date,
                lower_bound_inclusive_date=None,
                supervision_periods=[preceding_supervision_period],
            )
        )

        self.assertEqual(
            StateSupervisionPeriodSupervisionType.PAROLE,
            supervision_type,
        )

    def test_most_recent_supervision_type_lower_bound(self) -> None:
        preceding_supervision_period = StateSupervisionPeriod.new_with_defaults(
            state_code="US_ID",
            supervision_period_id=1,
            start_date=self.upper_bound_date - relativedelta(days=100),
            termination_date=self.upper_bound_date - relativedelta(days=10),
            supervision_type=StateSupervisionPeriodSupervisionType.PAROLE,
        )

        supervision_type = (
            us_id_get_most_recent_supervision_type_before_upper_bound_day(
                upper_bound_exclusive_date=self.upper_bound_date,
                lower_bound_inclusive_date=None,
                supervision_periods=[preceding_supervision_period],
            )
        )

        self.assertEqual(
            StateSupervisionPeriodSupervisionType.PAROLE,
            supervision_type,
        )

    def test_most_recent_supervision_type_ignore_before_lower_bound(self) -> None:
        preceding_supervision_period = StateSupervisionPeriod.new_with_defaults(
            state_code="US_ID",
            supervision_period_id=1,
            start_date=self.upper_bound_date - relativedelta(days=100),
            termination_date=self.upper_bound_date - relativedelta(days=10),
            supervision_type=StateSupervisionPeriodSupervisionType.PAROLE,
        )

        supervision_type = (
            us_id_get_most_recent_supervision_type_before_upper_bound_day(
                upper_bound_exclusive_date=self.upper_bound_date,
                lower_bound_inclusive_date=self.upper_bound_date
                - relativedelta(days=2),
                supervision_periods=[preceding_supervision_period],
            )
        )

        self.assertEqual(None, supervision_type)

    @freeze_time("2000-01-31")
    def test_most_recent_supervision_type_active_supervision(self) -> None:
        preceding_supervision_period = StateSupervisionPeriod.new_with_defaults(
            state_code="US_ID",
            supervision_period_id=1,
            start_date=date.today() - relativedelta(days=100),
            termination_date=None,
            supervision_type=StateSupervisionPeriodSupervisionType.PROBATION,
        )

        supervision_type = (
            us_id_get_most_recent_supervision_type_before_upper_bound_day(
                upper_bound_exclusive_date=self.upper_bound_date,
                lower_bound_inclusive_date=None,
                supervision_periods=[preceding_supervision_period],
            )
        )

        self.assertEqual(
            StateSupervisionPeriodSupervisionType.PROBATION,
            supervision_type,
        )

    def test_most_recent_supervision_type_overlapping_supervision(self) -> None:
        preceding_supervision_period = StateSupervisionPeriod.new_with_defaults(
            state_code="US_ID",
            supervision_period_id=1,
            start_date=self.upper_bound_date - relativedelta(days=100),
            termination_date=self.upper_bound_date + relativedelta(days=100),
            supervision_type=StateSupervisionPeriodSupervisionType.PROBATION,
        )

        supervision_type = (
            us_id_get_most_recent_supervision_type_before_upper_bound_day(
                upper_bound_exclusive_date=self.upper_bound_date,
                lower_bound_inclusive_date=None,
                supervision_periods=[preceding_supervision_period],
            )
        )

        self.assertEqual(
            StateSupervisionPeriodSupervisionType.PROBATION,
            supervision_type,
        )

    def test_most_recent_supervision_type_supervision_terminates_on_bound(self) -> None:
        preceding_supervision_period = StateSupervisionPeriod.new_with_defaults(
            state_code="US_ID",
            supervision_period_id=1,
            start_date=self.upper_bound_date - relativedelta(days=100),
            termination_date=self.upper_bound_date,
            supervision_type=StateSupervisionPeriodSupervisionType.PAROLE,
        )

        supervision_type = (
            us_id_get_most_recent_supervision_type_before_upper_bound_day(
                upper_bound_exclusive_date=self.upper_bound_date,
                lower_bound_inclusive_date=None,
                supervision_periods=[preceding_supervision_period],
            )
        )

        self.assertEqual(
            StateSupervisionPeriodSupervisionType.PAROLE,
            supervision_type,
        )

    def test_most_recent_supervision_type_supervision_starts_on_bound(self) -> None:
        preceding_supervision_period = StateSupervisionPeriod.new_with_defaults(
            state_code="US_ID",
            supervision_period_id=1,
            start_date=self.upper_bound_date,
            termination_date=self.upper_bound_date + relativedelta(years=1),
            supervision_type=StateSupervisionPeriodSupervisionType.PAROLE,
        )

        supervision_type = (
            us_id_get_most_recent_supervision_type_before_upper_bound_day(
                upper_bound_exclusive_date=self.upper_bound_date,
                lower_bound_inclusive_date=None,
                supervision_periods=[preceding_supervision_period],
            )
        )

        self.assertEqual(supervision_type, None)

    def test_most_recent_supervision_type_no_sentences_no_lower_bound(self) -> None:
        supervision_type = (
            us_id_get_most_recent_supervision_type_before_upper_bound_day(
                upper_bound_exclusive_date=self.upper_bound_date,
                lower_bound_inclusive_date=None,
                supervision_periods=[],
            )
        )

        self.assertEqual(supervision_type, None)

    def test_most_recent_supervision_type_no_sentences_same_day_bound(self) -> None:
        supervision_type = (
            us_id_get_most_recent_supervision_type_before_upper_bound_day(
                upper_bound_exclusive_date=self.upper_bound_date,
                lower_bound_inclusive_date=self.upper_bound_date,
                supervision_periods=[],
            )
        )

        self.assertEqual(supervision_type, None)
